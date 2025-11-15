# Based on breez_liquid.py

from importlib.util import find_spec

if not find_spec("breez_sdk_spark"):

    class BreezSparkSdkWallet:  # pyright: ignore
        def __init__(self):
            raise RuntimeError(
                "Breez Spark SDK is not installed. "
                "Ask admin to run `uv sync --extra breez-spark` to install it."
            )

else:
    import asyncio
    from asyncio import Queue
    from collections.abc import AsyncGenerator
    from pathlib import Path

    from bolt11 import decode as bolt11_decode
    from breez_sdk_spark import (
        ConnectRequest,
        EventListener,
        GetInfoResponse,
        GetPaymentRequest,
        Payment,
        PaymentDetails,
        PaymentMethod,
        PaymentState,
        PaymentType,
        PrepareReceiveRequest,
        PrepareSendRequest,
        ReceiveAmount,
        ReceivePaymentRequest,
        SdkEvent,
        SendPaymentRequest,
        connect,
        default_config,
    )
    from loguru import logger

    from lnbits.settings import settings

    from .base import (
        InvoiceResponse,
        PaymentFailedStatus,
        PaymentPendingStatus,
        PaymentResponse,
        PaymentStatus,
        PaymentSuccessStatus,
        StatusResponse,
        Wallet,
    )

    # Payment queues for managing incoming and outgoing payment events
    breez_spark_incoming_queue: Queue[PaymentDetails.LIGHTNING] = Queue()
    breez_spark_outgoing_queue: dict[str, Queue[PaymentDetails.LIGHTNING]] = {}

    class PaymentsListener(EventListener):
        """
        Event listener for Breez Spark SDK payment events.
        Routes payment success events to the appropriate queues for processing.
        """

        def on_event(self, e: SdkEvent) -> None:
            logger.debug(f"received breez spark sdk event: {e}")

            # Only process payment succeeded events with lightning payment details
            if not isinstance(e, SdkEvent.PAYMENT_SUCCEEDED) or not isinstance(
                e.details.details, PaymentDetails.LIGHTNING
            ):
                return

            payment = e.details
            payment_details = e.details.details

            # Route received payments to incoming queue
            if payment.payment_type is PaymentType.RECEIVE:
                breez_spark_incoming_queue.put_nowait(payment_details)
            # Route sent payments to their specific outgoing queue
            elif (
                payment.payment_type is PaymentType.SEND
                and payment_details.payment_hash in breez_spark_outgoing_queue
            ):
                breez_spark_outgoing_queue[payment_details.payment_hash].put_nowait(
                    payment_details
                )

    class BreezSparkSdkWallet(Wallet):  # type: ignore[no-redef]
        """
        Breez Spark SDK wallet implementation for nodeless Lightning payments.
        Provides Lightning Network functionality without running a full node.
        """

        def __init__(self):
            """
            Initialize the Breez Spark SDK wallet.

            Raises:
                ValueError: If required configuration settings are missing.
            """
            if not settings.breez_spark_seed:
                raise ValueError(
                    "cannot initialize BreezSparkSdkWallet: missing breez_spark_seed"
                )

            # Load API key from settings or .breez_spark file
            if not settings.breez_spark_api_key:
                breez_spark_api_key_file = Path("lnbits/wallets", ".breez_spark")
                if breez_spark_api_key_file.exists():
                    with open(breez_spark_api_key_file) as f:
                        settings.breez_spark_api_key = f.read().strip()

            # Initialize Spark network configuration with default settings
            self.config = default_config(
                breez_api_key=settings.breez_spark_api_key or "",
            )

            # Set private mode to prevent public node announcements
            self.config.private_mode = settings.breez_spark_private_mode

            # Create working directory for SDK data
            breez_sdk_working_dir = Path(
                settings.lnbits_data_folder, "breez-spark-sdk"
            )
            breez_sdk_working_dir.mkdir(parents=True, exist_ok=True)
            self.config.working_dir = breez_sdk_working_dir.absolute().as_posix()

            try:
                # Connect to Breez Spark network with mnemonic seed
                mnemonic = settings.breez_spark_seed
                connect_request = ConnectRequest(config=self.config, mnemonic=mnemonic)
                self.sdk_services = connect(connect_request)

                # Register event listener for payment notifications
                self.sdk_services.add_event_listener(PaymentsListener())

                logger.info("Breez Spark SDK wallet initialized successfully")
            except Exception as exc:
                logger.warning(exc)
                raise ValueError(
                    f"cannot initialize BreezSparkSdkWallet: {exc!s}"
                ) from exc

        async def cleanup(self):
            """
            Clean up resources and disconnect from the Breez Spark SDK.
            Called during application shutdown.
            """
            try:
                self.sdk_services.disconnect()
                logger.info("Breez Spark SDK wallet disconnected")
            except Exception as exc:
                logger.warning(f"Error during Breez Spark SDK cleanup: {exc}")

        async def status(self) -> StatusResponse:
            """
            Get the current wallet status and balance.

            Returns:
                StatusResponse: Contains error message (if any) and balance in millisats.
            """
            try:
                info: GetInfoResponse = self.sdk_services.get_info()
                balance_msat = int(info.wallet_info.balance_sat * 1000)
                logger.debug(f"Breez Spark wallet balance: {balance_msat} msat")
                return StatusResponse(None, balance_msat)
            except Exception as exc:
                logger.warning(f"Failed to get Breez Spark status: {exc}")
                return StatusResponse(f"Failed to connect to breez spark, got: '{exc}...'", 0)

        async def create_invoice(
            self,
            amount: int,
            memo: str | None = None,
            description_hash: bytes | None = None,
            unhashed_description: bytes | None = None,
            **_,
        ) -> InvoiceResponse:
            """
            Create a Lightning invoice for receiving payments.

            Args:
                amount: Amount to receive in satoshis.
                memo: Optional description for the invoice.
                description_hash: Optional hash of the description (not fully supported).
                unhashed_description: Optional unhashed description for description_hash.

            Returns:
                InvoiceResponse: Contains the invoice details or error message.
            """
            try:
                # Prepare receive request to calculate fees
                # Note: Breez SDK Spark expects amount in satoshis, not millisats
                receive_amount = ReceiveAmount.BITCOIN(amount)
                prepare_req = self.sdk_services.prepare_receive_payment(
                    PrepareReceiveRequest(
                        payment_method=PaymentMethod.BOLT11_INVOICE,
                        amount=receive_amount,  # type: ignore
                    )
                )
                receive_fees_sats = prepare_req.fees_sat

                # Build description from memo or unhashed_description
                description = memo or (
                    unhashed_description.decode() if unhashed_description else ""
                )

                # Create the invoice
                res = self.sdk_services.receive_payment(
                    ReceivePaymentRequest(
                        prepare_response=prepare_req,
                        description=description,
                        use_description_hash=description_hash is not None,
                    )
                )

                bolt11 = res.destination
                invoice_data = bolt11_decode(bolt11)
                payment_hash = invoice_data.payment_hash

                logger.debug(
                    f"Created Breez Spark invoice: {payment_hash} for {amount} sats"
                )

                return InvoiceResponse(
                    ok=True,
                    checking_id=payment_hash,
                    payment_request=bolt11,
                    fee_msat=receive_fees_sats * 1000,
                )
            except Exception as e:
                logger.warning(f"Failed to create Breez Spark invoice: {e}")
                return InvoiceResponse(ok=False, error_message=str(e))

        async def pay_invoice(
            self, bolt11: str, fee_limit_msat: int
        ) -> PaymentResponse:
            """
            Pay a Lightning invoice.

            Args:
                bolt11: The BOLT11 invoice string to pay.
                fee_limit_msat: Maximum fee willing to pay in millisats.

            Returns:
                PaymentResponse: Contains payment result or error message.
            """
            try:
                invoice_data = bolt11_decode(bolt11)
            except Exception as exc:
                logger.warning(f"Invalid BOLT11 invoice: {exc}")
                return PaymentResponse(
                    ok=False, error_message=f"Invalid BOLT11 invoice: {exc}"
                )

            try:
                # Prepare payment to calculate fees
                prepare_req = PrepareSendRequest(destination=bolt11)
                req = self.sdk_services.prepare_send_payment(prepare_req)

                # Convert fee limit from msat to sat for comparison
                fee_limit_sat = int(fee_limit_msat / 1000)

                # Check if fees exceed the limit
                if req.fees_sat and req.fees_sat > fee_limit_sat:
                    return PaymentResponse(
                        ok=False,
                        error_message=(
                            f"fee of {req.fees_sat} sat exceeds limit of "
                            f"{fee_limit_sat} sat"
                        ),
                    )

                # Execute payment
                send_response = self.sdk_services.send_payment(
                    SendPaymentRequest(prepare_response=req)
                )

            except Exception as exc:
                logger.warning(f"Exception while paying invoice: {exc}")
                return PaymentResponse(error_message=f"Exception while payment: {exc}")

            payment: Payment = send_response.payment
            logger.debug(f"Breez Spark pay invoice result: {payment}")
            checking_id = invoice_data.payment_hash

            fees = req.fees_sat * 1000 if req.fees_sat and req.fees_sat > 0 else 0

            # If payment is not immediately complete, wait for confirmation
            if payment.status != PaymentState.COMPLETE:
                return await self._wait_for_outgoing_payment(checking_id, fees, 10)

            # Verify payment details are available
            if not isinstance(payment.details, PaymentDetails.LIGHTNING):
                return PaymentResponse(
                    error_message="lightning payment details are not available"
                )

            return PaymentResponse(
                ok=True,
                checking_id=checking_id,
                fee_msat=payment.fees_sat * 1000,
                preimage=payment.details.preimage,
            )

        async def get_invoice_status(self, checking_id: str) -> PaymentStatus:
            """
            Check the status of a received payment (invoice).

            Args:
                checking_id: The payment hash of the invoice.

            Returns:
                PaymentStatus: Current status of the payment.
            """
            try:
                req = GetPaymentRequest.PAYMENT_HASH(checking_id)
                payment = self.sdk_services.get_payment(req=req)  # type: ignore

                if payment is None:
                    return PaymentPendingStatus()

                if payment.payment_type != PaymentType.RECEIVE:
                    logger.warning(f"unexpected payment type: {payment.payment_type}")
                    return PaymentPendingStatus()

                if payment.status == PaymentState.FAILED:
                    return PaymentFailedStatus()

                if payment.status == PaymentState.COMPLETE and isinstance(
                    payment.details, PaymentDetails.LIGHTNING
                ):
                    return PaymentSuccessStatus(
                        paid=True,
                        fee_msat=int(payment.fees_sat * 1000),
                        preimage=payment.details.preimage,
                    )

                return PaymentPendingStatus()
            except Exception as exc:
                logger.warning(f"Error checking invoice status: {exc}")
                return PaymentPendingStatus()

        async def get_payment_status(self, checking_id: str) -> PaymentStatus:
            """
            Check the status of a sent payment.

            Args:
                checking_id: The payment hash of the payment.

            Returns:
                PaymentStatus: Current status of the payment.
            """
            try:
                req = GetPaymentRequest.PAYMENT_HASH(checking_id)
                payment = self.sdk_services.get_payment(req=req)  # type: ignore

                if payment is None:
                    return PaymentPendingStatus()

                if payment.payment_type != PaymentType.SEND:
                    logger.warning(f"unexpected payment type: {payment.payment_type}")
                    return PaymentPendingStatus()

                if payment.status == PaymentState.COMPLETE:
                    if not isinstance(payment.details, PaymentDetails.LIGHTNING):
                        logger.warning("payment details are not of type LIGHTNING")
                        return PaymentPendingStatus()
                    return PaymentSuccessStatus(
                        fee_msat=int(payment.fees_sat * 1000),
                        preimage=payment.details.preimage,
                    )

                if payment.status == PaymentState.FAILED:
                    return PaymentFailedStatus()

                return PaymentPendingStatus()
            except Exception as exc:
                logger.warning(f"Error checking payment status: {exc}")
                return PaymentPendingStatus()

        async def paid_invoices_stream(self) -> AsyncGenerator[str, None]:
            """
            Stream of paid invoices (received payments).
            Yields payment hashes as invoices are paid.

            Yields:
                str: Payment hash of each paid invoice.
            """
            while settings.lnbits_running:
                details = await breez_spark_incoming_queue.get()
                logger.debug(f"breez spark invoice paid event: {details}")

                if not details.invoice:
                    logger.warning(
                        "Paid invoices stream expected bolt11 invoice, got None"
                    )
                    continue

                invoice_data = bolt11_decode(details.invoice)
                yield invoice_data.payment_hash

        async def _wait_for_outgoing_payment(
            self, checking_id: str, fees: int, timeout: int
        ) -> PaymentResponse:
            """
            Wait for an outgoing payment to complete.
            Used when payment is not immediately confirmed.

            Args:
                checking_id: The payment hash to wait for.
                fees: Expected fees in millisats.
                timeout: Maximum seconds to wait.

            Returns:
                PaymentResponse: Payment result after waiting or timeout.
            """
            logger.debug(f"waiting for outgoing payment {checking_id} to complete")
            try:
                breez_spark_outgoing_queue[checking_id] = Queue()
                payment_details = await asyncio.wait_for(
                    breez_spark_outgoing_queue[checking_id].get(), timeout
                )
                return PaymentResponse(
                    ok=True,
                    preimage=payment_details.preimage,
                    checking_id=checking_id,
                    fee_msat=fees,
                )
            except asyncio.TimeoutError:
                logger.debug(
                    f"payment '{checking_id}' is still pending after {timeout} seconds"
                )
                return PaymentResponse(
                    checking_id=checking_id,
                    fee_msat=fees,
                    error_message="payment is pending",
                )
            finally:
                breez_spark_outgoing_queue.pop(checking_id, None)
