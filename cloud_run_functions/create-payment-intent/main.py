"""
Create Payment Intent — creates a Stripe PaymentIntent for a $20 challenge stake.

Accepts POST { "user_id": "uuid" }
Returns { "client_secret": "...", "payment_intent_id": "...", "customer_id": "...", "ephemeral_key": "..." }

If the user has no stripe_customer_id yet, creates a Stripe Customer first.
"""

import json
import logging
import os

import functions_framework
import psycopg2
import psycopg2.extras
import stripe

logger = logging.getLogger(__name__)

stripe.api_key = os.environ.get("STRIPE_SECRET_KEY")

STRIPE_API_VERSION = "2024-06-20"


@functions_framework.http
def create_payment_intent(request):
    """HTTP Cloud Function entry point."""
    if request.method == "OPTIONS":
        return ("", 204, {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST",
            "Access-Control-Allow-Headers": "Content-Type",
        })

    headers = {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}

    if request.method != "POST":
        return (json.dumps({"error": "Method not allowed"}), 405, headers)

    body = request.get_json(silent=True)
    if not body or "user_id" not in body:
        return (json.dumps({"error": "user_id is required"}), 400, headers)

    user_id = body["user_id"]

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        return (json.dumps({"error": "DATABASE_URL not configured"}), 500, headers)

    try:
        conn = psycopg2.connect(database_url)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Look up existing stripe_customer_id
        cur.execute("SELECT stripe_customer_id FROM users WHERE id = %s", (user_id,))
        row = cur.fetchone()
        if not row:
            cur.close()
            conn.close()
            return (json.dumps({"error": "User not found"}), 404, headers)

        customer_id = row["stripe_customer_id"]

        # Create Stripe Customer if needed
        if not customer_id:
            customer = stripe.Customer.create(
                metadata={"supabase_user_id": user_id}
            )
            customer_id = customer.id
            cur.execute(
                "UPDATE users SET stripe_customer_id = %s WHERE id = %s",
                (customer_id, user_id),
            )
            conn.commit()

        # Create ephemeral key
        ephemeral_key = stripe.EphemeralKey.create(
            customer=customer_id,
            stripe_version=STRIPE_API_VERSION,
        )

        # Create PaymentIntent — $20 charge
        payment_intent = stripe.PaymentIntent.create(
            amount=2000,
            currency="usd",
            customer=customer_id,
            automatic_payment_methods={"enabled": True, "allow_redirects": "never"},
        )

        cur.close()
        conn.close()

        return (
            json.dumps({
                "client_secret": payment_intent.client_secret,
                "payment_intent_id": payment_intent.id,
                "customer_id": customer_id,
                "ephemeral_key": ephemeral_key.secret,
            }),
            200,
            headers,
        )

    except stripe.StripeError as e:
        logger.exception("Stripe error creating payment intent")
        return (json.dumps({"error": str(e)}), 500, headers)
    except Exception as e:
        logger.exception("Error creating payment intent")
        return (json.dumps({"error": str(e)}), 500, headers)
