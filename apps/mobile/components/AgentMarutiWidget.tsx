import { useEffect, useRef } from "react";
import {
  Animated,
  Easing,
  Image,
  Pressable,
  StyleSheet,
  Text,
  View,
} from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";

import { openContactForm } from "../lib/contactForm";
import { AGENT_NAME } from "../lib/appCopy";
import { premium } from "../theme/premium";

const agentAvatar = require("../assets/agent-maruti.jpg");

const AVATAR_SIZE = 64;
const RING_COUNT = 2;

function PulseRing({ delay }: { delay: number }) {
  const scale = useRef(new Animated.Value(0.9)).current;
  const opacity = useRef(new Animated.Value(0.4)).current;

  useEffect(() => {
    const animation = Animated.loop(
      Animated.sequence([
        Animated.delay(delay),
        Animated.parallel([
          Animated.timing(scale, {
            toValue: 1.35,
            duration: 2000,
            easing: Easing.out(Easing.quad),
            useNativeDriver: true,
          }),
          Animated.timing(opacity, {
            toValue: 0,
            duration: 2000,
            easing: Easing.out(Easing.quad),
            useNativeDriver: true,
          }),
        ]),
        Animated.parallel([
          Animated.timing(scale, { toValue: 0.9, duration: 0, useNativeDriver: true }),
          Animated.timing(opacity, { toValue: 0.4, duration: 0, useNativeDriver: true }),
        ]),
      ]),
    );
    animation.start();
    return () => animation.stop();
  }, [delay, opacity, scale]);

  return (
    <Animated.View
      pointerEvents="none"
      style={[styles.ring, { opacity, transform: [{ scale }] }]}
    />
  );
}

/** Compact floating agent — avatar + name only (no speech bubble). */
export function AgentMarutiWidget() {
  const insets = useSafeAreaInsets();

  return (
    <View
      pointerEvents="box-none"
      style={[styles.anchor, { bottom: insets.bottom + 10, right: 14 }]}
    >
      <Pressable
        accessibilityRole="button"
        accessibilityLabel="Contact Agent Maruti"
        onPress={openContactForm}
        style={({ pressed }) => [styles.pressable, pressed && styles.pressed]}
      >
        <View style={styles.avatarWrap}>
          {Array.from({ length: RING_COUNT }, (_, i) => (
            <PulseRing key={i} delay={i * 900} />
          ))}
          <View style={styles.avatarRing}>
            <Image source={agentAvatar} style={styles.avatar} accessibilityIgnoresInvertColors />
            <View style={styles.onlineDot} />
          </View>
        </View>
        <View style={styles.pill}>
          <Text style={styles.pillText}>{AGENT_NAME}</Text>
        </View>
      </Pressable>
    </View>
  );
}

const styles = StyleSheet.create({
  anchor: {
    position: "absolute",
    zIndex: 100,
    alignItems: "center",
  },
  pressable: {
    alignItems: "center",
    gap: 6,
  },
  pressed: {
    opacity: 0.9,
    transform: [{ scale: 0.97 }],
  },
  avatarWrap: {
    width: AVATAR_SIZE + 36,
    height: AVATAR_SIZE + 36,
    alignItems: "center",
    justifyContent: "center",
  },
  ring: {
    position: "absolute",
    width: AVATAR_SIZE + 14,
    height: AVATAR_SIZE + 14,
    borderRadius: (AVATAR_SIZE + 14) / 2,
    borderWidth: 2,
    borderColor: "rgba(34, 197, 94, 0.5)",
  },
  avatarRing: {
    width: AVATAR_SIZE,
    height: AVATAR_SIZE,
    borderRadius: AVATAR_SIZE / 2,
    borderWidth: 3,
    borderColor: "#86efac",
    backgroundColor: "#ecfdf5",
    alignItems: "center",
    justifyContent: "center",
    overflow: "hidden",
    shadowColor: premium.shadow,
    shadowOffset: { width: 0, height: 4 },
    shadowOpacity: 0.25,
    shadowRadius: 8,
    elevation: 5,
  },
  avatar: {
    width: AVATAR_SIZE - 4,
    height: AVATAR_SIZE - 4,
    borderRadius: (AVATAR_SIZE - 4) / 2,
  },
  onlineDot: {
    position: "absolute",
    right: 1,
    bottom: 1,
    width: 13,
    height: 13,
    borderRadius: 7,
    backgroundColor: "#22c55e",
    borderWidth: 2,
    borderColor: premium.surface,
  },
  pill: {
    backgroundColor: premium.ink,
    paddingHorizontal: 14,
    paddingVertical: 5,
    borderRadius: 20,
    shadowColor: premium.shadow,
    shadowOffset: { width: 0, height: 2 },
    shadowOpacity: 0.2,
    shadowRadius: 6,
    elevation: 3,
  },
  pillText: {
    color: "#fff",
    fontSize: 12,
    fontWeight: "800",
    letterSpacing: 0.3,
  },
});
