import { MaterialCommunityIcons } from "@expo/vector-icons";
import { useLocalSearchParams } from "expo-router";
import { useEffect, useState } from "react";
import { ActivityIndicator, ScrollView, StyleSheet, Text, View } from "react-native";
import { SafeAreaView } from "react-native-safe-area-context";

import { AppFooter } from "../../components/AppFooter";
import { ContactActionRow } from "../../components/ContactActionRow";
import { WINNER_PROFILE_HINT } from "../../lib/appCopy";
import { ContractorCard, getContractor } from "../../lib/api";
import { premium } from "../../theme/premium";

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

function Section({
  icon,
  label,
  children,
}: {
  icon: keyof typeof MaterialCommunityIcons.glyphMap;
  label: string;
  children: React.ReactNode;
}) {
  return (
    <View style={styles.section}>
      <View style={styles.sectionHead}>
        <MaterialCommunityIcons name={icon} size={18} color={premium.gold} />
        <Text style={styles.sectionLabel}>{label}</Text>
      </View>
      {children}
    </View>
  );
}

export default function ContractorDetailScreen() {
  const params = useLocalSearchParams<{ contractNo: string }>();
  const [contractor, setContractor] = useState<ContractorCard | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const data = await getContractor(params.contractNo);
        if (!cancelled) {
          setContractor(data);
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Could not load contractor");
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }
    load();
    return () => {
      cancelled = true;
    };
  }, [params.contractNo]);

  if (loading) {
    return (
      <SafeAreaView style={styles.center}>
        <View style={styles.loadingCard}>
          <ActivityIndicator color={premium.gold} size="large" />
          <Text style={styles.loadingText}>Opening profile…</Text>
        </View>
      </SafeAreaView>
    );
  }

  if (error || !contractor) {
    return (
      <SafeAreaView style={styles.center}>
        <MaterialCommunityIcons name="account-alert-outline" size={48} color={premium.inkFaint} />
        <Text style={styles.error}>{error || "Contractor not found"}</Text>
      </SafeAreaView>
    );
  }

  const phone = phoneWithCountryCode(contractor.seller_phone);
  const letter = initials(contractor.seller_name);

  return (
    <SafeAreaView style={styles.safe} edges={["bottom"]}>
      <ScrollView contentContainerStyle={styles.content} showsVerticalScrollIndicator={false}>
        <View style={styles.heroCard}>
          <View style={styles.avatarLarge}>
            <Text style={styles.avatarLargeText}>{letter}</Text>
          </View>
          <Text style={styles.seller}>{contractor.seller_name || "Unknown seller"}</Text>
          <View style={styles.valuePill}>
            <MaterialCommunityIcons name="cash-multiple" size={18} color={premium.success} />
            <Text style={styles.value}>{formatCurrency(contractor.contract_value)}</Text>
          </View>
          <Text style={styles.heroHint}>{WINNER_PROFILE_HINT}</Text>
        </View>

        <ContactActionRow phoneDigits={phone} email={contractor.seller_email ?? null} />

        <Section icon="package-variant" label="Product">
          <Text style={styles.body}>{contractor.product_name || "Product not listed"}</Text>
        </Section>

        <Section icon="file-certificate-outline" label="Contract">
          <Text style={styles.bodyMono}>{contractor.contract_no}</Text>
        </Section>

        {!!contractor.seller_address && (
          <Section icon="map-marker-outline" label="Address">
            <Text style={styles.body}>{contractor.seller_address}</Text>
          </Section>
        )}

        {!!contractor.seller_gstin && (
          <Section icon="identifier" label="GSTIN">
            <Text style={styles.bodyMono}>{contractor.seller_gstin}</Text>
          </Section>
        )}

        <AppFooter />
      </ScrollView>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  safe: {
    flex: 1,
    backgroundColor: premium.bgWarm,
  },
  center: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
    backgroundColor: premium.bgWarm,
    padding: 24,
    gap: 12,
  },
  loadingCard: {
    alignItems: "center",
    gap: 16,
    paddingVertical: 32,
    paddingHorizontal: 40,
    borderRadius: 22,
    backgroundColor: premium.surface,
    borderWidth: 1,
    borderColor: premium.borderStrong,
    shadowColor: premium.shadow,
    shadowOpacity: 0.3,
    shadowRadius: 20,
    shadowOffset: { width: 0, height: 8 },
    elevation: 3,
  },
  loadingText: {
    color: premium.inkSoft,
    fontWeight: "700",
    fontSize: 15,
  },
  content: {
    padding: 20,
    paddingBottom: 100,
    gap: 16,
  },
  heroCard: {
    alignItems: "center",
    paddingVertical: 24,
    paddingHorizontal: 20,
    borderRadius: 20,
    backgroundColor: premium.surface,
    borderWidth: 1,
    borderColor: premium.border,
    shadowColor: premium.shadow,
    shadowOpacity: 0.5,
    shadowRadius: 12,
    shadowOffset: { width: 0, height: 4 },
    elevation: 2,
    gap: 12,
  },
  avatarLarge: {
    width: 88,
    height: 88,
    borderRadius: 28,
    backgroundColor: premium.surfaceMuted,
    borderWidth: 2,
    borderColor: premium.borderStrong,
    alignItems: "center",
    justifyContent: "center",
  },
  avatarLargeText: {
    color: premium.gold,
    fontSize: 28,
    fontWeight: "900",
    letterSpacing: 1,
  },
  seller: {
    color: premium.ink,
    fontSize: 24,
    fontWeight: "900",
    textAlign: "center",
    letterSpacing: -0.5,
    lineHeight: 30,
  },
  valuePill: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
    paddingHorizontal: 14,
    paddingVertical: 8,
    borderRadius: 999,
    backgroundColor: "rgba(21, 128, 61, 0.1)",
  },
  value: {
    color: premium.success,
    fontSize: 17,
    fontWeight: "900",
  },
  heroHint: {
    color: premium.inkFaint,
    fontSize: 13,
    textAlign: "center",
    lineHeight: 19,
    fontWeight: "600",
    paddingHorizontal: 8,
  },
  section: {
    padding: 18,
    borderRadius: 20,
    backgroundColor: premium.surface,
    borderWidth: 1,
    borderColor: premium.border,
    gap: 10,
  },
  sectionHead: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
  },
  sectionLabel: {
    color: premium.inkFaint,
    fontSize: 11,
    fontWeight: "800",
    letterSpacing: 1.2,
    textTransform: "uppercase",
  },
  body: {
    color: premium.ink,
    fontSize: 16,
    lineHeight: 24,
    fontWeight: "500",
  },
  bodyMono: {
    color: premium.ink,
    fontSize: 15,
    lineHeight: 22,
    fontWeight: "700",
    letterSpacing: 0.3,
  },
  error: {
    color: premium.error,
    fontWeight: "700",
    textAlign: "center",
    fontSize: 16,
  },
});
