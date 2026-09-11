#!/usr/bin/env sh
set -eu

KITCHEN_API_URL="${KITCHEN_API_URL:-http://localhost:8080}"
SAMPLE_RESTAURANT_URL="${SAMPLE_RESTAURANT_URL:-http://localhost:8084}"
SAMPLE_RESTAURANT_OPERATOR_TOKEN="${SAMPLE_RESTAURANT_OPERATOR_TOKEN:-development-only-operator-secret}"
POLL_ATTEMPTS="${POLL_ATTEMPTS:-60}"

for command in curl jq; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "required command is missing: $command" >&2
        exit 1
    fi
done

response_file="$(mktemp)"
trap 'rm -f "$response_file"' EXIT

fail() {
    echo "smoke test failed: $1" >&2
    if [ -s "$response_file" ]; then
        cat "$response_file" >&2
        echo >&2
    fi
    exit 1
}

echo "1/7 checking restaurants"
curl -fsS "$KITCHEN_API_URL/v1/restaurants" -o "$response_file"
jq -e '.restaurants | any(.id == "rst-pizza-1")' "$response_file" >/dev/null ||
    fail "sample restaurant is missing"

echo "2/7 checking menu and availability"
curl -fsS "$KITCHEN_API_URL/v1/restaurants/rst-pizza-1/menu" -o "$response_file"
jq -e '
    ([.categories[].items[] | select(.id == "item-margherita" and .available == true)] | length == 1)
    and
    ([.categories[].items[] | select(.id == "item-lasagna" and .available == false)] | length == 1)
' "$response_file" >/dev/null || fail "menu availability is incorrect"

run_id="$(date +%s)-$$"
user_id="smoke-user-$run_id"
unavailable_key="smoke-unavailable-$run_id"
order_key="smoke-order-$run_id"

echo "3/7 checking unavailable item rejection"
unavailable_code="$(
    curl -sS -o "$response_file" -w '%{http_code}' \
        -X POST "$KITCHEN_API_URL/v1/orders" \
        -H 'Content-Type: application/json' \
        -H "X-User-ID: $user_id" \
        -H "Idempotency-Key: $unavailable_key" \
        --data '{"restaurantId":"rst-pizza-1","items":[{"menuItemId":"item-lasagna","quantity":1}]}'
)"
[ "$unavailable_code" = "400" ] || fail "unavailable item returned HTTP $unavailable_code"

echo "4/7 creating order"
create_code="$(
    curl -sS -o "$response_file" -w '%{http_code}' \
        -X POST "$KITCHEN_API_URL/v1/orders" \
        -H 'Content-Type: application/json' \
        -H "X-User-ID: $user_id" \
        -H "Idempotency-Key: $order_key" \
        --data '{"restaurantId":"rst-pizza-1","items":[{"menuItemId":"item-margherita","quantity":1,"modifierItemIds":["mod-extra-cheese"]}]}'
)"
[ "$create_code" = "201" ] || fail "create order returned HTTP $create_code"
order_id="$(jq -er '.id' "$response_file")" || fail "create order response has no id"

echo "5/7 replaying idempotent request"
replay_code="$(
    curl -sS -o "$response_file" -w '%{http_code}' \
        -X POST "$KITCHEN_API_URL/v1/orders" \
        -H 'Content-Type: application/json' \
        -H "X-User-ID: $user_id" \
        -H "Idempotency-Key: $order_key" \
        --data '{"restaurantId":"rst-pizza-1","items":[{"menuItemId":"item-margherita","quantity":1,"modifierItemIds":["mod-extra-cheese"]}]}'
)"
[ "$replay_code" = "200" ] || fail "idempotent replay returned HTTP $replay_code"
[ "$(jq -er '.id' "$response_file")" = "$order_id" ] || fail "idempotent replay returned another order"

echo "6/7 waiting for restaurant delivery"
delivered="false"
attempt=1
while [ "$attempt" -le "$POLL_ATTEMPTS" ]; do
    delivery_code="$(
        curl -sS -o "$response_file" -w '%{http_code}' \
            "$SAMPLE_RESTAURANT_URL/orders/$order_id" \
            -H "Authorization: Bearer $SAMPLE_RESTAURANT_OPERATOR_TOKEN"
    )"
    if [ "$delivery_code" = "200" ]; then
        delivered="true"
        break
    fi
    sleep 1
    attempt=$((attempt + 1))
done
[ "$delivered" = "true" ] || fail "order was not delivered to restaurant"

decision_code="$(
    curl -sS -o "$response_file" -w '%{http_code}' \
        -X POST "$SAMPLE_RESTAURANT_URL/orders/$order_id/status" \
        -H 'Content-Type: application/json' \
        -H "Authorization: Bearer $SAMPLE_RESTAURANT_OPERATOR_TOKEN" \
        --data '{"status":"accepted"}'
)"
case "$decision_code" in
    200|202) ;;
    *) fail "restaurant decision returned HTTP $decision_code" ;;
esac

echo "7/7 waiting for accepted order"
accepted="false"
attempt=1
while [ "$attempt" -le "$POLL_ATTEMPTS" ]; do
    order_code="$(
        curl -sS -o "$response_file" -w '%{http_code}' \
            "$KITCHEN_API_URL/v1/orders/$order_id" \
            -H "X-User-ID: $user_id"
    )"
    if [ "$order_code" = "200" ] && [ "$(jq -r '.status' "$response_file")" = "accepted" ]; then
        accepted="true"
        break
    fi
    sleep 1
    attempt=$((attempt + 1))
done
[ "$accepted" = "true" ] || fail "order did not reach accepted status"

echo "smoke test passed: order $order_id is accepted"
