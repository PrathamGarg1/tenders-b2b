import { Linking, Pressable, StyleSheet, Text, View } from "react-native";

import { PRIVACY_POLICY_URL } from "../lib/config";
import {
  DATA_SOURCE_LABEL,
  GEM_CONTRACTS_URL,
  GEM_HOME_URL,
  GOVERNMENT_DISCLAIMER,
} from "../lib/gemSources";
import { premium } from "../theme/premium";

function openUrl(url: string) {
  Linking.openURL(url).catch(() => undefined);
}

function openPrivacy() {
  if (PRIVACY_POLICY_URL) {
    openUrl(PRIVACY_POLICY_URL);
  }
}

function SourceLink({ label, url }: { label: string; url: string }) {
  return (
    <Pressable
      onPress={() => openUrl(url)}
      accessibilityRole="link"
      accessibilityLabel={`Open ${label}`}
    >
      <Text style={styles.sourceLink}>{label}</Text>
    </Pressable>
  );
}

export function AppFooter() {
  return (
    <View style={styles.wrap}>
      <View style={styles.disclaimerCard}>
        <Text style={styles.disclaimerTitle}>Disclaimer</Text>
        <Text style={styles.disclaimerBody}>{GOVERNMENT_DISCLAIMER}</Text>
        <Text style={styles.sourcesTitle}>{DATA_SOURCE_LABEL}</Text>
        <SourceLink label="gem.gov.in" url={GEM_HOME_URL} />
        <SourceLink label="gem.gov.in/view_contracts/contract_details" url={GEM_CONTRACTS_URL} />
      </View>
      {PRIVACY_POLICY_URL ? (
        <Pressable onPress={openPrivacy} accessibilityRole="link">
          <Text style={styles.privacy}>Privacy policy</Text>
        </Pressable>
      ) : null}
    </View>
  );
}

const styles = StyleSheet.create({
  wrap: {
    alignItems: "center",
    paddingTop: 24,
    paddingBottom: 8,
    gap: 10,
  },
  disclaimerCard: {
    width: "100%",
    maxWidth: 360,
    padding: 14,
    borderRadius: 14,
    backgroundColor: premium.surface,
    borderWidth: 1,
    borderColor: premium.border,
    gap: 6,
  },
  disclaimerTitle: {
    color: premium.ink,
    fontSize: 11,
    fontWeight: "800",
    letterSpacing: 1,
    textTransform: "uppercase",
  },
  disclaimerBody: {
    color: premium.inkSoft,
    fontSize: 13,
    lineHeight: 19,
    fontWeight: "600",
  },
  sourcesTitle: {
    marginTop: 4,
    color: premium.inkFaint,
    fontSize: 11,
    fontWeight: "800",
    letterSpacing: 0.6,
    textTransform: "uppercase",
  },
  sourceLink: {
    color: premium.accent,
    fontSize: 13,
    fontWeight: "700",
    lineHeight: 20,
  },
  privacy: {
    color: premium.accent,
    fontSize: 13,
    fontWeight: "700",
  },
});
