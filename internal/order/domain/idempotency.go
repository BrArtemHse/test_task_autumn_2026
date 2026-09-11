package orderdomain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

func FingerprintCreateOrder(input NewOrderInput) string {
	items := make([]string, 0, len(input.Items))
	for _, item := range input.Items {
		modifiers := append([]string(nil), item.ModifierItemIDs...)
		sort.Strings(modifiers)
		items = append(items, fmt.Sprintf(
			"%s:%s:%d:%d:%s",
			item.MenuItemID,
			item.Name,
			item.UnitPriceCents,
			item.Quantity,
			strings.Join(modifiers, ","),
		))
	}
	sort.Strings(items)

	canonical := strings.Join([]string{
		input.OrderID,
		input.UserID,
		input.RestaurantID,
		strings.Join(items, "|"),
	}, "\x00")

	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])
}
