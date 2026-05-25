import { Alert, Linking } from "react-native";

import { CONTACT_FORM_URL } from "./config";

export function openContactForm() {
  if (!CONTACT_FORM_URL) {
    Alert.alert(
      "Contact form not set up",
      "Add your Google Form link to EXPO_PUBLIC_CONTACT_FORM_URL in apps/mobile/.env, then restart the app.",
    );
    return;
  }
  Linking.openURL(CONTACT_FORM_URL).catch(() => undefined);
}
