import { MaterialCommunityIcons } from "@expo/vector-icons";
import { Linking, Pressable, StyleSheet, Text, View } from "react-native";

import { premium } from "../theme/premium";

type Props = {
  phoneDigits: string | null;
  email: string | null;
  /** Tighter layout for list cards */
  compact?: boolean;
};

function openUrl(url: string) {
  Linking.openURL(url).catch(() => undefined);
}

export function ContactActionRow({ phoneDigits, email, compact }: Props) {
  const iconSize = compact ? 22 : 26;
  const gap = compact ? 8 : 10;

  return (
    <View style={[styles.row, { gap }]}>
      <Pressable
        accessibilityRole="button"
        accessibilityLabel="Open WhatsApp"
        disabled={!phoneDigits}
        style={({ pressed }) => [
          styles.cell,
          styles.cellWhatsapp,
          compact && styles.cellCompact,
          !phoneDigits && styles.cellDisabled,
          pressed && phoneDigits && styles.pressed,
        ]}
        onPress={() => phoneDigits && openUrl(`whatsapp://send?phone=${phoneDigits}`)}
      >
        <MaterialCommunityIcons name="whatsapp" size={iconSize} color="#fff" />
        {!compact && <Text style={styles.caption}>WhatsApp</Text>}
      </Pressable>

      <Pressable
        accessibilityRole="button"
        accessibilityLabel="Call phone"
        disabled={!phoneDigits}
        style={({ pressed }) => [
          styles.cell,
          styles.cellPhone,
          compact && styles.cellCompact,
          !phoneDigits && styles.cellDisabled,
          pressed && phoneDigits && styles.pressed,
        ]}
        onPress={() => phoneDigits && openUrl(`tel:+${phoneDigits}`)}
      >
        <MaterialCommunityIcons name="phone" size={iconSize} color="#fff" />
        {!compact && <Text style={styles.caption}>Call</Text>}
      </Pressable>

      <Pressable
        accessibilityRole="button"
        accessibilityLabel="Send email"
        disabled={!email}
        style={({ pressed }) => [
          styles.cell,
          styles.cellEmail,
          compact && styles.cellCompact,
          !email && styles.cellDisabled,
          pressed && email && styles.pressed,
        ]}
        onPress={() => email && openUrl(`mailto:${email}`)}
      >
        <MaterialCommunityIcons name="email-outline" size={iconSize} color="#fff" />
        {!compact && <Text style={styles.caption}>Email</Text>}
      </Pressable>
    </View>
  );
}

const styles = StyleSheet.create({
  row: {
    flexDirection: "row",
    alignItems: "stretch",
  },
  cell: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
    minHeight: 52,
    borderRadius: 16,
    gap: 6,
    backgroundColor: premium.ink,
  },
  cellCompact: {
    minHeight: 46,
    borderRadius: 14,
  },
  cellWhatsapp: {
    backgroundColor: premium.whatsapp,
  },
  cellPhone: {
    backgroundColor: premium.accent,
  },
  cellEmail: {
    backgroundColor: premium.gold,
  },
  cellDisabled: {
    backgroundColor: "#d6d3d1",
  },
  pressed: {
    opacity: 0.88,
    transform: [{ scale: 0.98 }],
  },
  caption: {
    color: "#fff",
    fontSize: 11,
    fontWeight: "700",
    letterSpacing: 0.6,
    textTransform: "uppercase",
  },
});
