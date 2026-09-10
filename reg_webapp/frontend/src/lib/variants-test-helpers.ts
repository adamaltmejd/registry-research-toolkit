/**
 * Fixtures for the register-variant surfaces (Y-79), shared by the `variants.ts`
 * unit tests and the `VariantBrowser` / `VariantsSummary` / `CatalogNodeView`
 * browser tests so the `VariantsResponse` wire shape is spelled once (same
 * reason as `picker-test-helpers.ts`). Fully typed — a wire change fails HERE
 * rather than passing through an `as unknown as` cast in every suite.
 */
import type { VariantsResponse } from "./api";
import type { Variant, Version } from "./variants";

/** One `register_version` row as SCB delivers it: the same prose every year with
 * only the year moved on, which is what the fold collapses. */
export function lisaVersion(year: number, ageFrame: string): Version {
  return {
    name: `${year}`,
    description: `LISA ${year} innehåller uppgifter om individer.`,
    measurement_information: `Mätningen avser november ${year}.`,
    populations: [
      {
        name: `Individer ${ageFrame}`,
        definition: `Samtliga individer ${ageFrame} folkbokförda ${year}-12-31.`,
        comment: null,
        date_range: `${year}`,
      },
    ],
    object_types: [
      { name: "Individ", definition: `Folkbokförd person ${year}.` },
    ],
  };
}

/** Deliveries that carry nothing but their year — enough for a variant's SPAN,
 * which is all the register-page summary reads. */
export function datedVersions(...years: number[]): Version[] {
  return years.map((year) => ({
    name: `${year}`,
    description: null,
    measurement_information: null,
    populations: [],
    object_types: [],
  }));
}

/** One `register_variant` row; `fields` overrides any part of it. */
export function variant(slug: string, fields: Partial<Variant> = {}): Variant {
  return {
    slug,
    name: null,
    display_group: null,
    description: null,
    panel_entity_key: null,
    panel_time_grain: null,
    panel_time_key: null,
    variant_family: null,
    variant_family_label: null,
    versions: [],
    ...fields,
  };
}

/** A `GET /api/catalog/{provider}/{register}/variants` payload. */
export function variantsResponse(...variants: Variant[]): VariantsResponse {
  return { register: "scb/lisa", variants };
}
