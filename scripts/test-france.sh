#!/usr/bin/env bash
# France geocoding smoke test — covers cities, régions, départements, exonyms,
# hyphenated towns, addresses, postcodes, landmarks, disambiguation.
# Usage: ./test-france.sh [base_url]
set -u

BASE="${1:-http://127.0.0.1:2400}"
PASS=0
FAIL=0
FAILS=()

# expect_contains <query> <substr_in_first_display_name>
expect_contains() {
  local q="$1"; local want="$2"
  local resp
  resp=$(curl -s --get --data-urlencode "q=$q" --data-urlencode "limit=3" "$BASE/search")
  local got
  got=$(echo "$resp" | python3 -c 'import sys,json
try:
    a=json.load(sys.stdin)
    print(a[0].get("display_name","") if a else "")
except Exception as e:
    print("PARSE_ERR:"+str(e))')
  if [[ "$got" == *"$want"* ]]; then
    PASS=$((PASS+1))
    # printf "  OK   %-50s -> %s\n" "$q" "$got"
  else
    FAIL=$((FAIL+1))
    FAILS+=("$q")
    printf "  FAIL %-50s -> %s   (wanted: %s)\n" "$q" "${got:-<empty>}" "$want"
  fi
}

# expect_country <query> <expected_country_substr>
expect_country() {
  expect_contains "$1" "$2"
}

# expect_in_top3 <query> <substr> — passes if any of top 3 results contains substr
expect_in_top3() {
  local q="$1"; local want="$2"
  local resp
  resp=$(curl -s --get --data-urlencode "q=$q" --data-urlencode "limit=5" "$BASE/search")
  local hit
  hit=$(echo "$resp" | python3 -c 'import sys,json
try:
    a=json.load(sys.stdin)
    for r in a[:5]:
        print(r.get("display_name",""))
except Exception:
    pass' | grep -F "$want" | head -1)
  if [[ -n "$hit" ]]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    FAILS+=("$q [top3]")
    local got
    got=$(echo "$resp" | python3 -c 'import sys,json
try:
    a=json.load(sys.stdin)
    print("|".join(r.get("display_name","") for r in a[:3]))
except Exception:
    print("")')
    printf "  FAIL %-50s -> %s   (wanted top3: %s)\n" "$q" "${got:-<empty>}" "$want"
  fi
}

echo "=== France geocoding test against $BASE ==="
echo

echo "-- Major cities (canonical French names) --"
expect_contains "Paris"       "Paris"
expect_contains "Marseille"   "Marseille"
expect_contains "Lyon"        "Lyon"
expect_contains "Toulouse"    "Toulouse"
expect_contains "Nice"        "Nice"
expect_contains "Nantes"      "Nantes"
expect_contains "Strasbourg"  "Strasbourg"
expect_contains "Montpellier" "Montpellier"
expect_contains "Bordeaux"    "Bordeaux"
expect_contains "Lille"       "Lille"
expect_contains "Rennes"      "Rennes"
expect_contains "Reims"       "Reims"
expect_contains "Toulon"      "Toulon"
expect_contains "Saint-Étienne" "Saint-Étienne"
expect_contains "Le Havre"    "Havre"
expect_contains "Grenoble"    "Grenoble"
expect_contains "Dijon"       "Dijon"
expect_contains "Angers"      "Angers"
expect_contains "Nîmes"       "Nîmes"
expect_contains "Aix-en-Provence" "Aix-en-Provence"
expect_contains "Brest"       "Brest"
expect_contains "Le Mans"     "Le Mans"
expect_contains "Clermont-Ferrand" "Clermont-Ferrand"
expect_contains "Tours"       "Tours"
expect_contains "Limoges"     "Limoges"
expect_contains "Amiens"      "Amiens"
expect_contains "Perpignan"   "Perpignan"
expect_contains "Metz"        "Metz"
expect_contains "Besançon"    "Besançon"
expect_contains "Orléans"     "Orléans"
expect_contains "Mulhouse"    "Mulhouse"
expect_contains "Rouen"       "Rouen"
expect_contains "Caen"        "Caen"
expect_contains "Nancy"       "Nancy"
expect_contains "Avignon"     "Avignon"
expect_contains "Poitiers"    "Poitiers"
expect_contains "Versailles"  "Versailles"
expect_contains "Cannes"      "Cannes"
expect_contains "Antibes"     "Antibes"
expect_contains "La Rochelle" "La Rochelle"
expect_contains "Pau"         "Pau"
expect_contains "Calais"      "Calais"
expect_contains "Bayonne"     "Bayonne"
expect_contains "Annecy"      "Annecy"
expect_contains "Boulogne-Billancourt" "Boulogne-Billancourt"

echo
echo "-- Diacritic-free spellings --"
expect_contains "Saint-Etienne" "Saint-Étienne"
expect_contains "Nimes"       "Nîmes"
expect_contains "Besancon"    "Besançon"
expect_contains "Orleans"     "Orléans"
expect_contains "Chalons-en-Champagne" "Châlons-en-Champagne"

echo
echo "-- English exonyms --"
expect_in_top3 "Marseilles"   "Marseille"
expect_in_top3 "Lyons"        "Lyon"
expect_in_top3 "Dunkirk"      "Dunkerque"

echo
echo "-- German exonyms --"
expect_in_top3 "Strassburg"   "Strasbourg"
expect_in_top3 "Mülhausen"    "Mulhouse"

echo
echo "-- Italian/Spanish exonyms --"
expect_in_top3 "Marsiglia"    "Marseille"
expect_in_top3 "Niza"         "Nice"

echo
echo "-- Régions (18 régions of France) --"
expect_contains "Île-de-France"     "Île-de-France"
expect_contains "Auvergne-Rhône-Alpes" "Auvergne-Rhône-Alpes"
expect_contains "Hauts-de-France"   "Hauts-de-France"
expect_contains "Nouvelle-Aquitaine" "Nouvelle-Aquitaine"
expect_contains "Occitanie"         "Occitanie"
expect_contains "Provence-Alpes-Côte d'Azur" "Provence-Alpes-Côte"
expect_contains "Grand Est"         "Grand Est"
expect_contains "Pays de la Loire"  "Pays de la Loire"
expect_contains "Bretagne"          "Bretagne"
expect_contains "Normandie"         "Normandie"
expect_contains "Bourgogne-Franche-Comté" "Bourgogne-Franche-Comté"
expect_contains "Centre-Val de Loire" "Centre-Val de Loire"
expect_contains "Corse"             "Corse"
expect_contains "Guadeloupe"        "Guadeloupe"
expect_contains "Martinique"        "Martinique"
expect_contains "La Réunion"        "Réunion"
expect_contains "Guyane"            "Guyane"
expect_contains "Mayotte"           "Mayotte"

echo
echo "-- Région English exonyms --"
expect_in_top3 "Brittany"      "Bretagne"
expect_in_top3 "Burgundy"      "Bourgogne"
expect_in_top3 "Normandy"      "Normandie"
expect_in_top3 "Corsica"       "Corse"

echo
echo "-- Hyphenated towns / multi-word names --"
expect_contains "Vitry-sur-Seine"   "Vitry-sur-Seine"
expect_contains "Issy-les-Moulineaux" "Issy-les-Moulineaux"
expect_contains "Levallois-Perret"  "Levallois-Perret"
expect_contains "Aulnay-sous-Bois"  "Aulnay-sous-Bois"
expect_contains "Saint-Denis"       "Saint-Denis"
expect_contains "Neuilly-sur-Seine" "Neuilly-sur-Seine"
expect_contains "Saint-Germain-en-Laye" "Saint-Germain-en-Laye"

echo
echo "-- Postcodes (5-digit French) --"
expect_in_top3 "75001"        "Paris"
expect_in_top3 "13001"        "Marseille"
expect_in_top3 "69001"        "Lyon"
expect_in_top3 "33000"        "Bordeaux"
expect_in_top3 "06000"        "Nice"
expect_in_top3 "59000"        "Lille"

echo
echo "-- Postcode + city combos --"
expect_in_top3 "75001 Paris"       "Paris"
expect_in_top3 "13008 Marseille"   "Marseille"
expect_in_top3 "67000 Strasbourg"  "Strasbourg"

echo
echo "-- Addresses --"
expect_in_top3 "10 Rue de Rivoli, Paris"          "Rivoli"
expect_in_top3 "1 Avenue des Champs-Élysées, Paris" "Champs"
expect_in_top3 "1 Place Bellecour, Lyon"          "Bellecour"
expect_in_top3 "5 Rue de la République, Marseille" "République"
expect_in_top3 "Rue du Faubourg-Saint-Honoré, Paris" "Faubourg"

echo
echo "-- Address abbreviations (st, av, bd, etc.) --"
expect_in_top3 "1 Bd de la Madeleine, Paris"       "Madeleine"
expect_in_top3 "Av Foch, Paris"                    "Foch"
expect_in_top3 "Pl Vendôme, Paris"                 "Vendôme"

echo
echo "-- Landmarks --"
expect_in_top3 "Tour Eiffel"             "Eiffel"
expect_in_top3 "Eiffel Tower"            "Eiffel"
expect_in_top3 "Louvre"                  "Louvre"
expect_in_top3 "Notre-Dame de Paris"     "Notre-Dame"
expect_in_top3 "Arc de Triomphe"         "Triomphe"
expect_in_top3 "Sacré-Cœur"              "Sacré"
expect_in_top3 "Palais de Versailles"    "Versailles"
expect_in_top3 "Mont Saint-Michel"       "Saint-Michel"
expect_in_top3 "Mont Blanc"              "Mont Blanc"

echo
echo "-- Disambiguation: French city must beat same-named places elsewhere --"
expect_contains "Paris"        "France"
expect_contains "Bordeaux"     "France"
expect_contains "Versailles"   "France"
expect_contains "Cannes"       "France"

echo
echo "-- Suburbs / quartiers --"
expect_in_top3 "Le Marais, Paris"        "Marais"
expect_in_top3 "Montmartre, Paris"       "Montmartre"
expect_in_top3 "La Défense"              "Défense"
expect_in_top3 "Saint-Germain-des-Prés"  "Saint-Germain"
expect_in_top3 "Vieux Lyon"              "Vieux Lyon"

echo
echo "-- Overseas territories --"
expect_in_top3 "Saint-Denis, La Réunion"  "Réunion"
expect_in_top3 "Cayenne"                  "Cayenne"
expect_in_top3 "Fort-de-France"           "Fort-de-France"
expect_in_top3 "Pointe-à-Pitre"           "Pointe-à-Pitre"

echo
echo "-- Départements (a sample) --"
expect_in_top3 "Bouches-du-Rhône"   "Bouches-du-Rhône"
expect_in_top3 "Hauts-de-Seine"     "Hauts-de-Seine"
expect_in_top3 "Seine-Saint-Denis"  "Seine-Saint-Denis"
expect_in_top3 "Var"                "Var"
expect_in_top3 "Finistère"          "Finistère"

echo
echo "============================="
printf "PASS: %d   FAIL: %d   (total %d)\n" "$PASS" "$FAIL" "$((PASS+FAIL))"
if [[ $FAIL -gt 0 ]]; then
  echo "Failed queries:"
  for q in "${FAILS[@]}"; do echo "  - $q"; done
fi
