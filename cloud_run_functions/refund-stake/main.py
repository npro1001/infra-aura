"""
Refund Stake — issues a full Stripe refund for a completed challenge.

Accepts POST { "challenge_id": "uuid" }
Returns { "status": "refunded", "refund_id": "re_xxx" }

Idempotent: if stripe_refund_id is already set, returns success without calling Stripe.
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


@functions_framework.http
def refund_stake(request):
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
    if not body or "challenge_id" not in body:
        return (json.dumps({"error": "challenge_id is required"}), 400, headers)

    challenge_id = body["challenge_id"]

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        return (json.dumps({"error": "DATABASE_URL not configured"}), 500, headers)

    try:
        conn = psycopg2.connect(database_url)
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Fetch challenge
        cur.execute(
            "SELECT id, status, stripe_pi_id, mock_paid, stripe_refund_id FROM challenges WHERE id = %s",
            (challenge_id,),
        )
        challenge = cur.fetchone()

        if not challenge:
            cur.close()
            conn.close()
            return (json.dumps({"error": "Challenge not found"}), 404, headers)

        # Idempotency: already refunded
        if challenge["stripe_refund_id"]:
            cur.close()
            conn.close()
            return (
                json.dumps({
                    "status": "refunded",
                    "refund_id": challenge["stripe_refund_id"],
                }),
                200,
                headers,
            )

        # Validate refund eligibility
        if challenge["status"] != "completed":
            cur.close()
            conn.close()
            return (json.dumps({"error": "Challenge is not completed"}), 400, headers)

        if challenge["mock_paid"]:
            cur.close()
            conn.close()
            return (json.dumps({"error": "Cannot refund mock-paid challenge"}), 400, headers)

        if not challenge["stripe_pi_id"]:
            cur.close()
            conn.close()
            return (json.dumps({"error": "No payment intent on record"}), 400, headers)

        # Issue full refund
        try:
            refund = stripe.Refund.create(payment_intent=challenge["stripe_pi_id"])
            cur.execute(
                "UPDATE challenges SET stripe_refund_id = %s, refund_status = 'succeeded' WHERE id = %s",
                (refund.id, challenge_id),
            )
            conn.commit()
            cur.close()
            conn.close()

            return (
                json.dumps({"status": "refunded", "refund_id": refund.id}),
                200,
                headers,
            )

        except stripe.StripeError as e:
            logger.exception("Stripe refund failed")
            cur.execute(
                "UPDATE challenges SET refund_status = 'failed' WHERE id = %s",
                (challenge_id,),
            )
            conn.commit()
            cur.close()
            conn.close()
            return (json.dumps({"error": str(e)}), 500, headers)

    except Exception as e:
        logger.exception("Error processing refund")
        return (json.dumps({"error": str(e)}), 500, headers)
