import { useCallback, useEffect, useRef, useState } from "react";
import {
  Dimensions,
  FlatList,
  Image,
  NativeScrollEvent,
  NativeSyntheticEvent,
  StyleSheet,
  Text,
  View,
  ViewToken,
} from "react-native";

import { USE_CASE_AUTO_ADVANCE_MS, USE_CASE_SLIDES, UseCaseSlide } from "../lib/useCaseSlides";
import { premium } from "../theme/premium";

const HORIZONTAL_PAD = 20;
const CARD_GAP = 10;
const screenWidth = Dimensions.get("window").width;
const CARD_WIDTH = screenWidth - HORIZONTAL_PAD * 2;
const CARD_HEIGHT = 156;
const SNAP = CARD_WIDTH + CARD_GAP;

function UseCaseCard({ slide }: { slide: UseCaseSlide }) {
  return (
    <View style={[styles.card, { width: CARD_WIDTH }]}>
      <Image source={slide.image} style={styles.image} resizeMode="cover" accessibilityIgnoresInvertColors />
      <View style={styles.overlay} />
      <Text style={styles.label}>{slide.label}</Text>
    </View>
  );
}

type Props = {
  /** When false, carousel sits flush at top with no heading */
  showSectionTitle?: boolean;
  sectionTitle?: string;
};

export function UseCaseImageCarousel({
  showSectionTitle = false,
  sectionTitle = "What you can do here",
}: Props) {
  const listRef = useRef<FlatList<UseCaseSlide>>(null);
  const [activeIndex, setActiveIndex] = useState(0);
  const indexRef = useRef(0);

  const onViewableItemsChanged = useRef(({ viewableItems }: { viewableItems: ViewToken[] }) => {
    const i = viewableItems[0]?.index;
    if (i != null && i >= 0) {
      indexRef.current = i;
      setActiveIndex(i);
    }
  }).current;

  const viewabilityConfig = useRef({ viewAreaCoveragePercentThreshold: 55 }).current;

  const scrollTo = useCallback((index: number) => {
    listRef.current?.scrollToOffset({ offset: index * SNAP, animated: true });
    indexRef.current = index;
    setActiveIndex(index);
  }, []);

  useEffect(() => {
    const timer = setInterval(() => {
      const next = (indexRef.current + 1) % USE_CASE_SLIDES.length;
      scrollTo(next);
    }, USE_CASE_AUTO_ADVANCE_MS);
    return () => clearInterval(timer);
  }, [scrollTo]);

  const onScrollEnd = (e: NativeSyntheticEvent<NativeScrollEvent>) => {
    const i = Math.round(e.nativeEvent.contentOffset.x / SNAP);
    const clamped = Math.max(0, Math.min(i, USE_CASE_SLIDES.length - 1));
    indexRef.current = clamped;
    setActiveIndex(clamped);
  };

  return (
    <View style={[styles.wrap, !showSectionTitle && styles.wrapFlushTop]}>
      {showSectionTitle ? <Text style={styles.sectionTitle}>{sectionTitle}</Text> : null}
      <FlatList
        ref={listRef}
        data={USE_CASE_SLIDES}
        horizontal
        showsHorizontalScrollIndicator={false}
        decelerationRate="fast"
        snapToInterval={SNAP}
        snapToAlignment="start"
        disableIntervalMomentum
        contentContainerStyle={styles.listContent}
        keyExtractor={(item) => item.id}
        onViewableItemsChanged={onViewableItemsChanged}
        viewabilityConfig={viewabilityConfig}
        onMomentumScrollEnd={onScrollEnd}
        getItemLayout={(_, index) => ({
          length: SNAP,
          offset: SNAP * index,
          index,
        })}
        renderItem={({ item, index }) => (
          <View style={index < USE_CASE_SLIDES.length - 1 ? styles.cardSpacer : undefined}>
            <UseCaseCard slide={item} />
          </View>
        )}
      />
      <View style={styles.dots}>
        {USE_CASE_SLIDES.map((slide, i) => (
          <View key={slide.id} style={[styles.dot, i === activeIndex && styles.dotActive]} />
        ))}
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  wrap: {
    marginHorizontal: -HORIZONTAL_PAD,
    marginTop: 4,
  },
  wrapFlushTop: {
    marginTop: 0,
    marginBottom: 4,
  },
  sectionTitle: {
    marginHorizontal: HORIZONTAL_PAD,
    marginBottom: 10,
    color: premium.inkSoft,
    fontSize: 12,
    fontWeight: "800",
    letterSpacing: 1,
    textTransform: "uppercase",
  },
  listContent: {
    paddingHorizontal: HORIZONTAL_PAD,
  },
  cardSpacer: {
    marginRight: CARD_GAP,
  },
  card: {
    height: CARD_HEIGHT,
    borderRadius: 18,
    overflow: "hidden",
    backgroundColor: premium.ink,
    borderWidth: 1,
    borderColor: premium.border,
  },
  image: {
    ...StyleSheet.absoluteFillObject,
    width: "100%",
    height: "100%",
  },
  overlay: {
    ...StyleSheet.absoluteFillObject,
    backgroundColor: "rgba(28, 25, 23, 0.35)",
  },
  label: {
    position: "absolute",
    left: 16,
    right: 16,
    bottom: 14,
    color: "#fff",
    fontSize: 18,
    fontWeight: "800",
    letterSpacing: -0.3,
  },
  dots: {
    flexDirection: "row",
    justifyContent: "center",
    gap: 4,
    marginTop: 10,
  },
  dot: {
    width: 5,
    height: 5,
    borderRadius: 3,
    backgroundColor: premium.inkFaint,
    opacity: 0.35,
  },
  dotActive: {
    width: 14,
    backgroundColor: premium.gold,
    opacity: 1,
  },
});
