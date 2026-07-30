#!/bin/sh
set -eu

network=${KUBO_CONTROL_NETWORK:-bloar-kubo-control}
subnet=${KUBO_CONTROL_SUBNET:-172.30.189.0/28}
gateway=${KUBO_CONTROL_GATEWAY:-172.30.189.1}

case "$network" in
	"" | *[!A-Za-z0-9_.-]*)
		echo "invalid KUBO_CONTROL_NETWORK: $network" >&2
		exit 1
		;;
esac

if docker network inspect "$network" >/dev/null 2>&1; then
	driver=$(docker network inspect --format '{{.Driver}}' "$network")
	internal=$(docker network inspect --format '{{.Internal}}' "$network")
	actual_subnet=$(docker network inspect --format '{{(index .IPAM.Config 0).Subnet}}' "$network")
	actual_gateway=$(docker network inspect --format '{{(index .IPAM.Config 0).Gateway}}' "$network")
	if [ "$driver" != bridge ] || [ "$internal" != true ] ||
		[ "$actual_subnet" != "$subnet" ] || [ "$actual_gateway" != "$gateway" ]; then
		echo "$network exists with driver=$driver internal=$internal subnet=$actual_subnet gateway=$actual_gateway" >&2
		echo "refusing to reuse it; expected bridge true $subnet $gateway" >&2
		exit 1
	fi
else
	docker network create \
		--driver bridge \
		--internal \
		--subnet "$subnet" \
		--gateway "$gateway" \
		"$network" >/dev/null
fi

# A loopback listener is the ordinary pre-migration Kubo API and the desired
# gateway listener is an idempotent re-run. Anything else on the RPC port can
# shadow or conflict with Kubo, so stop before changing its configuration.
if command -v ss >/dev/null 2>&1; then
	listeners=$(ss -H -ltn 'sport = :5001' | awk '{print $4}')
	if [ -n "$listeners" ]; then
		while IFS= read -r address; do
			case "$address" in
				127.0.0.1:5001 | "[::1]:5001" | "$gateway:5001")
					;;
				*)
					echo "refusing Kubo control setup: unexpected listener on $address" >&2
					exit 1
					;;
			esac
		done <<EOF
$listeners
EOF
	fi
fi

echo "Kubo control network ready: $network ($subnet, gateway $gateway)"
