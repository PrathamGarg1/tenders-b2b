import { MaterialCommunityIcons } from "@expo/vector-icons";
import { Pressable, StyleSheet, Text, TextInput, View } from "react-native";

import { premium } from "../theme/premium";

type Props = {
  value: string;
  onChangeText: (value: string) => void;
  onSubmit: () => void;
  loading?: boolean;
  placeholder?: string;
};

export function SearchBar({
  value,
  onChangeText,
  onSubmit,
  loading,
  placeholder = "Product, contract no., seller…",
}: Props) {
  return (
    <View style={styles.wrap}>
      <View style={styles.inputShell}>
        <MaterialCommunityIcons name="magnify" size={22} color={premium.inkFaint} style={styles.inputIcon} />
        <TextInput
          value={value}
          onChangeText={onChangeText}
          onSubmitEditing={onSubmit}
          placeholder={placeholder}
          placeholderTextColor={premium.inkFaint}
          returnKeyType="search"
          autoCapitalize="none"
          autoCorrect={false}
          style={styles.input}
        />
      </View>
      <Pressable
        onPress={onSubmit}
        disabled={loading || !value.trim()}
        style={({ pressed }) => [
          styles.button,
          pressed && styles.buttonPressed,
          (!value.trim() || loading) && styles.buttonDisabled,
        ]}
      >
        <MaterialCommunityIcons
          name={loading ? "progress-clock" : "arrow-right"}
          size={22}
          color="#fff"
          style={styles.buttonIcon}
        />
        <Text style={styles.buttonText}>{loading ? "Searching" : "Search"}</Text>
      </Pressable>
    </View>
  );
}

const styles = StyleSheet.create({
  wrap: {
    gap: 12,
  },
  inputShell: {
    flexDirection: "row",
    alignItems: "center",
    minHeight: 52,
    borderRadius: 16,
    borderWidth: 1,
    borderColor: premium.borderStrong,
    backgroundColor: premium.surface,
    paddingHorizontal: 4,
  },
  inputIcon: {
    marginLeft: 12,
  },
  input: {
    flex: 1,
    minHeight: 56,
    paddingHorizontal: 10,
    fontSize: 17,
    color: premium.ink,
    fontWeight: "500",
  },
  button: {
    minHeight: 50,
    borderRadius: 16,
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "center",
    gap: 8,
    backgroundColor: premium.ink,
    borderWidth: 1,
    borderColor: "rgba(255,255,255,0.12)",
  },
  buttonPressed: {
    opacity: 0.9,
    transform: [{ scale: 0.99 }],
  },
  buttonDisabled: {
    backgroundColor: "#d6d3d1",
    borderColor: "transparent",
  },
  buttonIcon: {
    marginTop: 1,
  },
  buttonText: {
    color: "#fff",
    fontSize: 17,
    fontWeight: "700",
    letterSpacing: 0.3,
  },
});
