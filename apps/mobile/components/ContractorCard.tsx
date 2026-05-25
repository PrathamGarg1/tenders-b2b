import { Link } from "expo-router";
import { MaterialCommunityIcons } from "@expo/vector-icons";
import { StyleSheet, Text, View } from "react-native";

import { ContractorCard as ContractorCardType } from "../lib/api";
import { premium } from "../theme/premium";
import { ContactActionRow } from "./ContactActionRow";

type Props = {
  contractor: ContractorCardType;
};

function formatCurrency(value?: number | null): string {
  if (!value || Number.isNaN(value)) {
    return "Value not listed";
  }
  return new Intl.NumberFormat("en-IN", {
    style: "currency",
    currency: "INR",
    maximumFractionDigits: 0,
  }).format(value);
}

function phoneWithCountryCode(phone?: string | null): string | null {
  if (!phone) {
    return null;
  }
  const digits = phone.replace(/\D/g, "");
  if (!digits) {
    return null;
  }
  return digits.length === 10 ? `91${digits}` : digits;
}

function initials(name?: string | null): string {
  if (!name?.trim()) {
    return "?";
  }
  const parts = name.trim().split(/\s+/).filter(Boolean);
  if (parts.length === 1) {
    return parts[0].slice(0, 2).toUpperCase();
  }
  return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
}

export function ContractorCard({ contractor }: Props) {
  const phone = phoneWithCountryCode(contractor.seller_phone);
  const letter = initials(contractor.seller_name);

  return (
    <View style={styles.card}>
      <View style={styles.topRow}>
        <View style={styles.avatar}>
          <Text style={styles.avatarText}>{letter}</Text>
        </View>
        <View style={styles.topBody}>
          <Text style={styles.seller} numberOfLines={2}>
            {contractor.seller_name || "Unknown seller"}
          </Text>
          <View style={styles.valuePill}>
            <MaterialCommunityIcons name="trending-up" size={14} color={premium.success} />
            <Text style={styles.value}>{formatCurrency(contractor.contract_value)}</Text>
          </View>
        </View>
      </View>

      <View style={styles.divider} />

      <Text style={styles.productLabel}>Awarded product</Text>
      <Text style={styles.product} numberOfLines={3}>
        {contractor.product_name || "Product not listed"}
      </Text>

      <View style={styles.metaRow}>
        <MaterialCommunityIcons name="file-document-outline" size={14} color={premium.inkFaint} />
        <Text style={styles.meta} numberOfLines={1}>
          {contractor.contract_no}
        </Text>
      </View>

      <ContactActionRow phoneDigits={phone} email={contractor.seller_email ?? null} compact />

      <Link href={`/contractor/${contractor.contract_no}`} style={styles.linkRow}>
        <Text style={styles.linkText}>View contractor</Text>
        <MaterialCommunityIcons name="chevron-right" size={20} color={premium.gold} />
      </Link>
    </View>
  );
}

const styles = StyleSheet.create({
  card: {
    gap: 10,
    padding: 16,
    borderRadius: 20,
    backgroundColor: premium.surface,
    borderWidth: 1,
    borderColor: premium.border,
    shadowColor: premium.shadow,
    shadowOpacity: 1,
    shadowRadius: 12,
    shadowOffset: { width: 0, height: 4 },
    elevation: 2,
  },
  topRow: {
    flexDirection: "row",
    gap: 14,
    alignItems: "flex-start",
  },
  avatar: {
    width: 52,
    height: 52,
    borderRadius: 16,
    backgroundColor: premium.surfaceMuted,
    borderWidth: 1,
    borderColor: premium.borderStrong,
    alignItems: "center",
    justifyContent: "center",
  },
  avatarText: {
    color: premium.gold,
    fontSize: 17,
    fontWeight: "800",
    letterSpacing: 0.5,
  },
  topBody: {
    flex: 1,
    gap: 8,
  },
  seller: {
    color: premium.ink,
    fontSize: 18,
    fontWeight: "800",
    letterSpacing: -0.3,
    lineHeight: 24,
  },
  valuePill: {
    flexDirection: "row",
    alignItems: "center",
    alignSelf: "flex-start",
    gap: 6,
    paddingHorizontal: 10,
    paddingVertical: 5,
    borderRadius: 999,
    backgroundColor: "rgba(21, 128, 61, 0.08)",
  },
  value: {
    color: premium.success,
    fontSize: 14,
    fontWeight: "800",
  },
  divider: {
    height: 1,
    backgroundColor: premium.border,
    marginVertical: 2,
  },
  productLabel: {
    color: premium.inkFaint,
    fontSize: 11,
    fontWeight: "800",
    letterSpacing: 1.2,
    textTransform: "uppercase",
  },
  product: {
    color: premium.inkSoft,
    fontSize: 15,
    lineHeight: 22,
    fontWeight: "500",
  },
  metaRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
  },
  meta: {
    flex: 1,
    color: premium.inkFaint,
    fontSize: 12,
    fontWeight: "600",
  },
  linkRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "center",
    gap: 2,
    marginTop: 4,
    paddingVertical: 10,
    borderTopWidth: StyleSheet.hairlineWidth,
    borderTopColor: premium.border,
  },
  linkText: {
    color: premium.ink,
    fontSize: 14,
    fontWeight: "700",
    letterSpacing: 0.2,
  },
});
