# Northstar Studio Storefront

This is a legal automation starter app for online revenue operations.

This app provides a production-style digital storefront pipeline:

- curated product catalog with brand-focused storefront pages
- product and bundle checkout with optional order bump
- lead capture for abandoned-cart follow-up
- Stripe checkout + webhook sale confirmation
- post-purchase delivery emails (SMTP or simulated mode)
- payout queue processing to Venmo via PayPal Payouts

## Stack

- Python 3.10+
- Flask
- SQLite

## Brand + Product Strategy

- Public home page: `/`
- Catalog: `/store`
- Product page: `/products/<product_id>`
- Bundle page: `/bundle/<bundle_key>`
- Market analysis notes: `MARKET_ANALYSIS.md`

## Quick start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
export $(grep -v '^#' .env | xargs)
python app/main.py
```

Open: `http://localhost:8080`

## Go live now (Render)

1. Push this repo to GitHub.
2. In Render, click **New +** -> **Blueprint**.
3. Select your GitHub repo; Render will detect `render.yaml` and deploy.
4. Set required secrets in Render: `APP_PUBLIC_URL`, `STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET`, `PAYPAL_CLIENT_ID`, `PAYPAL_CLIENT_SECRET`, `PAYOUT_SENDER_EMAIL`.
5. (Optional) set SMTP vars for automated delivery emails: `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`, `SMTP_FROM`.
5. Open your Render service URL once deployment finishes.

Notes:
- This uses `DATABASE_PATH=/tmp/revenue_bot.db` on Render free tier (ephemeral storage).
- For persistence, switch to Render Postgres or paid disk.

## Key endpoints

- `GET /` landing page
- `GET /store` storefront
- `GET /health`
- `GET /api/products`
- `GET /products/<product_id>`
- `GET /bundle/<bundle_key>`
- `GET /checkout/<product_id>`
- `GET /checkout/bundle/<bundle_key>`
- `POST /capture-lead/product/<product_id>`
- `POST /capture-lead/bundle/<bundle_key>`
- `GET /admin` dashboard
- `POST /admin/generate` (header: `x-admin-token`)
- `POST /admin/generate-batch?count=10` (header: `x-admin-token`)
- `POST /admin/run-payouts` (header: `x-admin-token`)
- `GET /admin/leads` (header: `x-admin-token`)
- `GET /admin/deliveries` (header: `x-admin-token`)
- `POST /webhooks/stripe`

## Stripe checkout requirements

Set:

- `STRIPE_SECRET_KEY` (`sk_test_...` in sandbox, `sk_live_...` in production)
- `STRIPE_WEBHOOK_SECRET` from your webhook destination (`whsec_...`)
- `APP_PUBLIC_URL` to your deployed app URL (example: `https://revenue-bot-ktqu.onrender.com`)

Each product checkout link now points to `/checkout/<product_id>` and redirects customers to a Stripe-hosted checkout page.

## Stripe webhook format expected

`POST /webhooks/stripe` expects `checkout.session.completed` payload with:

- `data.object.amount_total`
- `data.object.currency`
- `data.object.metadata.product_id` or `data.object.metadata.bundle_key`
- `data.object.metadata.lead_id` (optional)
- `data.object.metadata.order_bump` (`yes`/`no`)
- `data.object.customer_details.email` (for delivery email)

And header `Stripe-Signature`, verified against `STRIPE_WEBHOOK_SECRET`.

## Venmo payouts

The app uses PayPal Payouts with `recipient_wallet=VENMO`.

You must configure:

- `PAYPAL_CLIENT_ID`
- `PAYPAL_CLIENT_SECRET`
- `PAYPAL_ENV` (`sandbox` or `live`)
- `PAYOUT_SENDER_EMAIL`

If PayPal credentials are missing, payouts are marked `simulated` so you can test end-to-end behavior safely.

## Notes

- Cover assets are in `static/covers/`.
- If SMTP is not configured, delivery emails are logged as simulated.
