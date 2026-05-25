import { Stack } from "expo-router";
import { StatusBar } from "expo-status-bar";
import { View } from "react-native";

import { AgentMarutiWidget } from "../components/AgentMarutiWidget";
import { APP_HEADER_TITLE, PROFILE_HEADER_TITLE } from "../lib/appCopy";
import { premium } from "../theme/premium";

export default function RootLayout() {
  return (
    <View style={{ flex: 1, backgroundColor: premium.bgWarm }}>
      <Stack
        screenOptions={{
          headerStyle: { backgroundColor: premium.bgElevated },
          headerTintColor: premium.ink,
          headerShadowVisible: false,
          headerTitleStyle: {
            fontWeight: "800",
            fontSize: 17,
          },
          contentStyle: { backgroundColor: premium.bgWarm },
        }}
      >
        <Stack.Screen name="index" options={{ title: APP_HEADER_TITLE }} />
        <Stack.Screen name="contractor/[contractNo]" options={{ title: PROFILE_HEADER_TITLE }} />
      </Stack>
      <AgentMarutiWidget />
      <StatusBar style="dark" />
    </View>
  );
}
