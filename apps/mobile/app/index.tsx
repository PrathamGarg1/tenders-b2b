import { MaterialCommunityIcons } from "@expo/vector-icons";
import { useState } from "react";
import { ActivityIndicator, FlatList, Keyboard, StyleSheet, Text, View } from "react-native";
import { SafeAreaView } from "react-native-safe-area-context";

import { AppFooter } from "../components/AppFooter";
import { ContractorCard } from "../components/ContractorCard";
import { SearchBar } from "../components/SearchBar";
import { UseCaseImageCarousel } from "../components/UseCaseImageCarousel";
import {
  EMPTY_HINT,
  EMPTY_TITLE,
  HOME_BLURB,
  HOME_KICKER,
  HOME_TITLE,
  LOADING_LABEL,
  RESULTS_LABEL,
  SEARCH_PLACEHOLDER,
} from "../lib/appCopy";
import { ContractorCard as ContractorCardType, searchContractors } from "../lib/api";
import { premium } from "../theme/premium";

export default function HomeScreen() {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<ContractorCardType[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [searched, setSearched] = useState(false);

  async function runSearch() {
    const cleanQuery = query.trim();
    if (!cleanQuery || loading) {
      return;
    }
    Keyboard.dismiss();
    setLoading(true);
    setError(null);
    setSearched(true);
    try {
      const response = await searchContractors(cleanQuery, 20);
      setResults(response.results);
    } catch (err) {
      setResults([]);
      setError(err instanceof Error ? err.message : "Search failed");
    } finally {
      setLoading(false);
    }
  }

  return (
    <SafeAreaView style={styles.safe} edges={["bottom"]}>
      <FlatList
        contentContainerStyle={styles.content}
        data={results}
        keyExtractor={(item) => item.contract_no}
        keyboardShouldPersistTaps="handled"
        ListHeaderComponent={
          <View style={styles.hero}>
            <UseCaseImageCarousel />
            <Text style={styles.kicker}>{HOME_KICKER}</Text>
            <Text style={styles.title}>{HOME_TITLE}</Text>
            <Text style={styles.blurb}>{HOME_BLURB}</Text>
            <SearchBar
              value={query}
              onChangeText={setQuery}
              onSubmit={runSearch}
              loading={loading}
              placeholder={SEARCH_PLACEHOLDER}
            />
            {loading && (
              <View style={styles.loaderRow}>
                <ActivityIndicator color={premium.gold} size="small" />
                <Text style={styles.loaderCopy}>{LOADING_LABEL}</Text>
              </View>
            )}
            {error && (
              <View style={styles.errorBanner}>
                <MaterialCommunityIcons name="alert-circle-outline" size={20} color={premium.error} />
                <Text style={styles.error}>{error}</Text>
              </View>
            )}
            {searched && !loading && !error && (
              <View style={styles.countRow}>
                <MaterialCommunityIcons name="trophy-outline" size={18} color={premium.gold} />
                <Text style={styles.count}>
                  {results.length} {RESULTS_LABEL}
                </Text>
              </View>
            )}
          </View>
        }
        ListFooterComponent={<AppFooter />}
        ListEmptyComponent={
          searched && !loading && !error ? (
            <View style={styles.emptyWrap}>
              <MaterialCommunityIcons name="trophy-outline" size={48} color={premium.inkFaint} />
              <Text style={styles.emptyTitle}>{EMPTY_TITLE}</Text>
              <Text style={styles.empty}>{EMPTY_HINT}</Text>
            </View>
          ) : null
        }
        renderItem={({ item }) => <ContractorCard contractor={item} />}
        ItemSeparatorComponent={() => <View style={styles.separator} />}
      />
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  safe: {
    flex: 1,
    backgroundColor: premium.bgWarm,
  },
  content: {
    padding: 20,
    paddingBottom: 100,
  },
  hero: {
    gap: 10,
    marginBottom: 16,
  },
  kicker: {
    marginTop: 4,
    color: premium.gold,
    fontSize: 12,
    fontWeight: "800",
    letterSpacing: 0.8,
    textTransform: "uppercase",
  },
  title: {
    color: premium.ink,
    fontSize: 32,
    fontWeight: "800",
    letterSpacing: -1.2,
    lineHeight: 36,
  },
  blurb: {
    color: premium.inkSoft,
    fontSize: 16,
    lineHeight: 23,
    fontWeight: "500",
    maxWidth: 340,
  },
  loaderRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 10,
    marginTop: 4,
  },
  loaderCopy: {
    color: premium.inkSoft,
    fontWeight: "600",
    fontSize: 14,
  },
  errorBanner: {
    flexDirection: "row",
    alignItems: "flex-start",
    gap: 10,
    padding: 12,
    borderRadius: 14,
    backgroundColor: "rgba(185, 28, 28, 0.08)",
    borderWidth: 1,
    borderColor: "rgba(185, 28, 28, 0.2)",
  },
  error: {
    flex: 1,
    color: premium.error,
    fontWeight: "700",
    fontSize: 14,
    lineHeight: 20,
  },
  countRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
  },
  count: {
    color: premium.inkSoft,
    fontWeight: "700",
    fontSize: 14,
  },
  emptyWrap: {
    alignItems: "center",
    marginTop: 28,
    paddingHorizontal: 12,
    gap: 8,
  },
  emptyTitle: {
    color: premium.ink,
    fontSize: 18,
    fontWeight: "800",
    marginTop: 4,
  },
  empty: {
    color: premium.inkSoft,
    textAlign: "center",
    fontSize: 15,
    lineHeight: 22,
    maxWidth: 300,
  },
  separator: {
    height: 14,
  },
});
