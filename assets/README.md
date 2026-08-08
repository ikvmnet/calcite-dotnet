# Logo

The crystal is Apache Calcite's own, lifted from `site/img/logo.svg` in the Calcite tree — facets,
white edges, sparkles and all — and recoloured to the .NET palette. The badge is the .NET rounded
square in `#512BD4`.

| file | use |
|---|---|
| `icon.svg` | crystal on the .NET badge — package icon, avatar, favicon |
| `icon.png` | 512×512 raster of `icon.svg`; the copy at the repository root is the `PackageIcon` for all three packages |
| `mark.svg` | the crystal alone, 512×512 |
| `mono.svg` | single-colour silhouette; change the one `fill` to recolour it |

There is deliberately no wordmark lockup. A mark setting "APACHE" and "calcite" in Calcite's own
arrangement reads as an Apache Software Foundation product, and this repository is not one.

The square canvases keep a safe area: the artwork's longer side is 80% of the box, except `icon.svg`,
whose badge is edge to edge by design with the crystal at 67% of the height.

## Colour

Calcite's six crystal tones are remapped twice — a blue-to-purple ramp for the free-standing mark,
and a light one for the crystal sitting on the purple badge.

| Calcite | mark | on badge |
|---|---|---|
| `#ffffff` | `#FFFFFF` | `#FFFFFF` |
| `#d5e5ff` | `#E4E4FF` | `#F5F1FF` |
| `#aaccff` | `#A6C6FF` | `#E3DAFF` |
| `#80b3ff` | `#86A8FF` | `#CBBBF9` |
| `#5599ff` | `#6E5EEA` | `#AE97F4` |
| `#2a7fff` | `#512BD4` | `#8869EC` |

The badge is `#512BD4`, the .NET purple.

## Provenance

The crystal is a derivative of the Apache Calcite logo, Copyright the Apache Software Foundation,
used under the Apache License 2.0; each SVG carries that notice. This is a community mark for this
repository — it is not an ASF mark and not a Microsoft one, and it does not imply endorsement by
either.
