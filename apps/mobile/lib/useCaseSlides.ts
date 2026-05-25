import { ImageSourcePropType } from "react-native";

export type UseCaseSlide = {
  id: string;
  label: string;
  image: ImageSourcePropType;
};

/** Order matches how users discover value — search first, then why contact winners */
export const USE_CASE_SLIDES: UseCaseSlide[] = [
  {
    id: "search",
    label: "Search any winner",
    image: require("../assets/use-cases/use-case-find-winner.png"),
  },
  {
    id: "raw",
    label: "Raw materials",
    image: require("../assets/use-cases/use-case-raw-materials.png"),
  },
  {
    id: "supply",
    label: "Supply to them",
    image: require("../assets/use-cases/use-case-supply.png"),
  },
  {
    id: "intel",
    label: "Award & contract info",
    image: require("../assets/use-cases/use-case-intel.png"),
  },
  {
    id: "contact",
    label: "Phone · WhatsApp",
    image: require("../assets/use-cases/use-case-contact.png"),
  },
  {
    id: "procure",
    label: "Procure anything",
    image: require("../assets/use-cases/use-case-procure.png"),
  },
];

export const USE_CASE_AUTO_ADVANCE_MS = 4200;
