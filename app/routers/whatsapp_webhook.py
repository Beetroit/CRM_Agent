import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from app.config import settings
from app.dependencies import get_producer_dependency
from app.services.rabbitmq_service import AsyncRabbitMQProducer
from app.services.whatsapp_service import WhatsAppService

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/webhook")
async def verify_webhook(
    request: Request,
    hub_mode: Optional[str] = None,
    hub_challenge: Optional[str] = None,
    hub_verify_token: Optional[str] = None,
):
    """
    Endpoint for WhatsApp webhook verification.
    """
    logger.info(
        f"Webhook verification request received: mode={hub_mode}, token={hub_verify_token}"
    )

    if hub_mode == "subscribe" and hub_verify_token == settings.WHATSAPP_VERIFY_TOKEN:
        logger.info("Webhook verification successful.")
        return Response(content=hub_challenge, media_type="text/plain")
    else:
        logger.error("Webhook verification failed: Invalid verify token or mode.")
        raise HTTPException(status_code=403, detail="Verification failed")


@router.post("/rev/twilio")
async def handle_webhook(
    request: Request,
    rabbitmq_producer: AsyncRabbitMQProducer = Depends(get_producer_dependency),
):
    """
    Endpoint to receive and process incoming WhatsApp messages from Twilio.
    """
    try:
        # Get raw request body
        raw_data = await request.body()

        # Create WhatsApp service instance
        whatsapp_service = WhatsAppService(rabbitmq_producer)

        # Process the webhook
        twiml_response = await whatsapp_service.handle_webhook(raw_data)

        return Response(content=twiml_response, media_type="application/xml")

    except Exception as e:
        logger.error(f"Webhook handler error: {e}")
        # Return empty TwiML response even on error to avoid webhook retries
        from twilio.twiml.messaging_response import MessagingResponse

        resp = MessagingResponse()
        return Response(content=str(resp), media_type="application/xml")
