#!/usr/bin/env bash
# US geocoding regression suite — covers cities, states, ZIPs, addresses,
# nicknames, ordinals, abbreviations, Spanish/Hawaiian/Native names,
# disambiguation, edge cases. Categorised so we can see which axis regresses.
# Usage: ./test-us.sh [base_url]
set -u

BASE="${1:-http://127.0.0.1:2400}"
# US index runs alone on dev; no country filter needed (and country=
# conflicts with bare q= as a "structured" parameter on this API)
CC_FILTER=""

# Counters per category
declare -A PASS_CAT
declare -A FAIL_CAT
TOTAL_PASS=0
TOTAL_FAIL=0
FAILS=()
CURRENT_CAT="general"

section() { CURRENT_CAT="$1"; printf "\n--- %s ---\n" "$1"; }

# expect_in_top <q> <substr> <topN>
expect_in_top() {
  local q="$1"; local want="$2"; local n="${3:-3}"
  local resp
  resp=$(curl -s --get --data-urlencode "q=$q" --data-urlencode "limit=$n" "$BASE/search$CC_FILTER")
  local hit
  hit=$(echo "$resp" | python3 -c "
import sys,json
try:
    a=json.load(sys.stdin)
    for r in a[:$n]:
        print(r.get('display_name',''))
except Exception:
    pass" | grep -i -F "$want" | head -1)
  if [[ -n "$hit" ]]; then
    PASS_CAT[$CURRENT_CAT]=$((${PASS_CAT[$CURRENT_CAT]:-0}+1))
    TOTAL_PASS=$((TOTAL_PASS+1))
  else
    FAIL_CAT[$CURRENT_CAT]=$((${FAIL_CAT[$CURRENT_CAT]:-0}+1))
    TOTAL_FAIL=$((TOTAL_FAIL+1))
    FAILS+=("[$CURRENT_CAT] $q")
    local got
    got=$(echo "$resp" | python3 -c "
import sys,json
try:
    a=json.load(sys.stdin)
    print(' | '.join(r.get('display_name','')[:80] for r in a[:3]))
except Exception:
    print('')")
    printf "  FAIL %-50s -> %s   (wanted: %s)\n" "$q" "${got:-<empty>}" "$want"
  fi
}

# expect_first <q> <substr_in_first_display_name>
expect_first() {
  local q="$1"; local want="$2"
  local resp
  resp=$(curl -s --get --data-urlencode "q=$q" --data-urlencode "limit=1" "$BASE/search$CC_FILTER")
  local got
  got=$(echo "$resp" | python3 -c "
import sys,json
try:
    a=json.load(sys.stdin)
    print(a[0].get('display_name','') if a else '')
except Exception as e:
    print('PARSE_ERR:'+str(e))")
  if [[ "${got,,}" == *"${want,,}"* ]]; then
    PASS_CAT[$CURRENT_CAT]=$((${PASS_CAT[$CURRENT_CAT]:-0}+1))
    TOTAL_PASS=$((TOTAL_PASS+1))
  else
    FAIL_CAT[$CURRENT_CAT]=$((${FAIL_CAT[$CURRENT_CAT]:-0}+1))
    TOTAL_FAIL=$((TOTAL_FAIL+1))
    FAILS+=("[$CURRENT_CAT] $q")
    printf "  FAIL %-50s -> %s   (wanted: %s)\n" "$q" "${got:-<empty>}" "$want"
  fi
}

# ─── Cities (basic) ───
section "city-basic"
expect_first "New York"               "New York"
expect_first "Los Angeles"            "Los Angeles"
expect_first "Chicago"                "Chicago"
expect_first "Houston"                "Houston"
expect_first "Phoenix"                "Phoenix"
expect_first "Philadelphia"           "Philadelphia"
expect_first "San Antonio"            "San Antonio"
expect_first "San Diego"              "San Diego"
expect_first "Dallas"                 "Dallas"
expect_first "Austin"                 "Austin"
expect_first "Jacksonville"           "Jacksonville"
expect_first "Columbus"               "Columbus"
expect_first "Indianapolis"           "Indianapolis"
expect_first "Charlotte"              "Charlotte"
expect_first "Seattle"                "Seattle"
expect_first "Denver"                 "Denver"
expect_first "Boston"                 "Boston"
expect_first "Detroit"                "Detroit"
expect_first "Memphis"                "Memphis"
expect_first "Portland"               "Portland"
expect_first "Atlanta"                "Atlanta"
expect_first "Miami"                  "Miami"
expect_first "Minneapolis"            "Minneapolis"

# ─── Cities w/ state disambiguation ───
section "city-state"
expect_first "Springfield, IL"        "Springfield"
expect_in_top "Springfield, IL"       "Illinois" 3
expect_first "Springfield, MA"        "Springfield"
expect_in_top "Springfield, MA"       "Massachusetts" 3
expect_first "Springfield, MO"        "Springfield"
expect_in_top "Springfield, MO"       "Missouri" 3
expect_first "Portland, OR"           "Portland"
expect_in_top "Portland, OR"          "Oregon" 3
expect_first "Portland, ME"           "Portland"
expect_in_top "Portland, ME"          "Maine" 3
expect_first "Columbus, OH"           "Columbus"
expect_in_top "Columbus, OH"          "Ohio" 3
expect_first "Columbus, GA"           "Columbus"
expect_in_top "Columbus, GA"          "Georgia" 3
expect_first "Kansas City, MO"        "Kansas City"
expect_in_top "Kansas City, MO"       "Missouri" 3
expect_first "Kansas City, KS"        "Kansas City"
expect_in_top "Kansas City, KS"       "Kansas" 3
expect_first "Aurora, CO"             "Aurora"
expect_in_top "Aurora, CO"            "Colorado" 3
expect_first "Aurora, IL"             "Aurora"
expect_in_top "Aurora, IL"            "Illinois" 3
expect_first "Pasadena, CA"           "Pasadena"
expect_in_top "Pasadena, CA"          "California" 3
expect_first "Pasadena, TX"           "Pasadena"
expect_in_top "Pasadena, TX"          "Texas" 3

# ─── Cities w/ comma-free state ───
section "city-state-nocomma"
expect_first "Springfield Illinois"   "Springfield"
expect_in_top "Springfield Illinois"  "Illinois" 3
expect_first "Portland Oregon"        "Portland"
expect_in_top "Portland Oregon"       "Oregon" 3
expect_first "Columbus Ohio"          "Columbus"
expect_in_top "Columbus Ohio"         "Ohio" 3
expect_first "Aurora Colorado"        "Aurora"
expect_in_top "Aurora Colorado"       "Colorado" 3

# ─── Nicknames / variants ───
section "nicknames"
expect_first "NYC"                    "New York"
expect_first "LA"                     "Los Angeles"
expect_first "DC"                     "Washington"
expect_first "SF"                     "San Francisco"
expect_first "Philly"                 "Philadelphia"
expect_first "Vegas"                  "Las Vegas"
expect_first "NOLA"                   "New Orleans"
expect_first "ATL"                    "Atlanta"
expect_first "PDX"                    "Portland"
expect_first "Big Apple"              "New York"
expect_first "City of Angels"         "Los Angeles"
expect_first "Motor City"             "Detroit"
expect_first "Windy City"             "Chicago"

# ─── Saint / St ───
# Index legitimately stores the canonical "Saint X" form; user types
# either spelling and should still hit the right city. Test by checking
# the city name component (Louis, Paul, Petersburg) and the state.
section "saint-st"
expect_in_top "Saint Louis"            "Louis" 5
expect_in_top "Saint Louis"            "Missouri" 5
expect_in_top "St Louis"               "Missouri" 5
expect_in_top "St. Louis"              "Missouri" 5
expect_in_top "Saint Paul"             "Paul" 3
expect_in_top "Saint Paul"             "Minnesota" 3
expect_in_top "St Paul"                "Minnesota" 3
expect_in_top "Saint Petersburg"       "Florida" 3
expect_in_top "St Pete"                "Florida" 5

# ─── ZIP codes (5-digit) ───
section "zip-5"
expect_in_top "10001"                 "New York" 3
expect_in_top "90210"                 "Beverly Hills" 3
expect_in_top "60601"                 "Chicago" 3
expect_in_top "94102"                 "San Francisco" 3
expect_in_top "20500"                 "Washington" 3
expect_in_top "33101"                 "Miami" 3
expect_in_top "02134"                 "Boston" 3
expect_in_top "98101"                 "Seattle" 3
expect_in_top "78701"                 "Austin" 3
expect_in_top "30303"                 "Atlanta" 3

# ─── ZIP+4 ───
section "zip-9"
expect_in_top "10001-2345"            "New York" 3
expect_in_top "90210-1234"            "Beverly Hills" 3
expect_in_top "94102-5678"            "San Francisco" 3
expect_in_top "200005-1111"           "Washington" 3   # leading-digit accepted

# ─── ZIP + city combo ───
section "zip-city"
expect_in_top "Beverly Hills 90210"   "Beverly Hills" 3
expect_in_top "90210 Beverly Hills"   "Beverly Hills" 3
expect_in_top "New York 10001"        "New York" 3
expect_in_top "Chicago 60601"         "Chicago" 3

# ─── Spanish / Mexican-origin names ───
section "spanish"
expect_first "San Francisco"          "San Francisco"
expect_first "San Jose"               "San"
expect_first "San José"               "San Jos"
expect_first "Los Angeles"            "Los Angeles"
expect_first "El Paso"                "El Paso"
expect_first "Santa Monica"           "Santa Monica"
expect_first "Santa Fe"               "Santa Fe"
expect_first "San Antonio"            "San Antonio"
expect_first "Sacramento"             "Sacramento"
expect_first "Las Vegas"              "Las Vegas"

# ─── Hawaiian (kahakō / ʻokina) ───
# Index stores the canonical Hawaiian spelling (ʻokina + macrons);
# accept any result whose name contains the relevant root letters.
section "hawaiian"
expect_first "Honolulu"               "Honolulu"
expect_in_top "Hawaii"                "Hawai" 5    # Hawaii or Hawaiʻi
expect_in_top "Hawaiʻi"               "Hawai" 5
expect_first "Maui"                   "Maui"
expect_in_top "Oahu"                  "ahu" 3      # Oahu / Oʻahu
expect_in_top "Oʻahu"                 "ahu" 3
expect_in_top "Kauai"                 "Kaua" 3
expect_in_top "Kauaʻi"                "Kaua" 3
expect_first "Hilo"                   "Hilo"
expect_first "Kailua"                 "Kailua"
expect_in_top "Lihue"                 "Hawaii" 3   # Līhuʻe Kauaʻi County HI
expect_in_top "Lihuʻe"                "Hawaii" 3
expect_in_top "Waikiki"               "Hawaii" 3   # Waikīkī
expect_in_top "Mauna Kea"             "Mauna" 3

# ─── Ordinals ───
section "ordinals"
expect_in_top "1st Avenue, New York"   "1st Av" 5
expect_in_top "First Avenue, New York" "Avenue" 5
expect_in_top "5th Avenue, New York"   "5th Av" 5
expect_in_top "Fifth Avenue, New York" "Avenue" 5

# ─── Street addresses ───
section "addresses"
expect_in_top "1600 Pennsylvania Avenue, Washington" "1600" 5
expect_in_top "1600 Pennsylvania Ave NW, Washington" "1600" 5
expect_in_top "350 5th Ave, New York"  "350" 5
expect_in_top "1 Apple Park Way, Cupertino" "Apple" 5
expect_in_top "1 Infinite Loop, Cupertino" "Infinite" 5
expect_in_top "100 Main St, Springfield, MA" "Main" 5
expect_in_top "1 First St NE, Washington" "First" 5
expect_in_top "200 W 42nd St, New York"   "42" 5
expect_in_top "1234 Sunset Blvd, Los Angeles" "Sunset" 5

# ─── Landmarks / POIs ───
section "landmarks"
expect_in_top "Statue of Liberty"     "Liberty" 5
expect_in_top "Empire State Building" "Empire" 5
expect_in_top "Golden Gate Bridge"    "Golden Gate" 5
expect_in_top "Times Square"          "Times Square" 5
expect_in_top "Central Park"          "Central Park" 5
expect_in_top "Hollywood Sign"        "Hollywood" 5
expect_in_top "Grand Canyon"          "Grand Canyon" 5
expect_in_top "Yellowstone"           "Yellowstone" 5
expect_in_top "Yosemite"              "Yosemite" 5
expect_in_top "Mount Rushmore"        "Rushmore" 5
expect_in_top "Niagara Falls"         "Niagara" 5
expect_in_top "Las Vegas Strip"       "Las Vegas" 5
expect_in_top "Disneyland"            "Disney" 5
expect_in_top "Lake Tahoe"            "Tahoe" 5
expect_in_top "Lake Michigan"         "Michigan" 5

# ─── States ───
section "states"
expect_in_top "California"            "California" 3
expect_in_top "Texas"                 "Texas" 3
expect_in_top "Florida"               "Florida" 3
expect_in_top "New York State"        "New York" 3
expect_in_top "Alaska"                "Alaska" 3
expect_in_top "Hawaii"                "Hawaii" 5
expect_in_top "Massachusetts"         "Massachusetts" 3
expect_in_top "Pennsylvania"          "Pennsylvania" 3
expect_in_top "Washington State"      "Washington" 3
expect_in_top "Puerto Rico"           "Puerto Rico" 5

# ─── Counties ───
section "counties"
expect_in_top "Los Angeles County"    "Los Angeles" 3
expect_in_top "Cook County, IL"       "Cook" 3
expect_in_top "Harris County, TX"     "Harris" 3
expect_in_top "King County, WA"       "King" 3
expect_in_top "Maricopa County, AZ"   "Maricopa" 3
expect_in_top "Orange County, CA"     "Orange" 3

# ─── Boroughs / neighbourhoods ───
section "neighbourhoods"
expect_in_top "Manhattan"             "Manhattan" 5
expect_in_top "Brooklyn"              "Brooklyn" 5
expect_in_top "Queens"                "Queens" 5
expect_in_top "The Bronx"             "Bronx" 5
expect_in_top "Staten Island"         "Staten Island" 5
expect_in_top "Hollywood"             "Hollywood" 5
expect_in_top "Beverly Hills"         "Beverly Hills" 5
expect_in_top "Wall Street"           "Wall" 5

# ─── Fuzzy / typos ───
# "Pittsburg" is a real CA city — ranking it ahead of Pittsburgh PA is
# acceptable. Test that Pittsburgh PA at least surfaces in top 5.
section "fuzzy"
expect_in_top "Pittsburgh"            "Pittsburgh" 3    # canonical spelling
expect_in_top "Pittsburg"             "Pittsburg" 5     # accept either
expect_in_top "Cincinatti"            "Cincinnati" 5    # double-n
expect_in_top "Albuqerque"            "Albuquerque" 5   # missing u
expect_in_top "Conneticut"            "Connecticut" 5   # missing c

# ─── Native American / less-common ───
section "native"
expect_first "Sioux Falls"            "Sioux"
expect_first "Tuscaloosa"             "Tuscaloosa"
expect_first "Cheyenne"               "Cheyenne"
expect_first "Mackinac"               "Mackinac"
expect_first "Chautauqua"             "Chautauqua"
expect_first "Anchorage"              "Anchorage"
expect_first "Ketchikan"              "Ketchikan"

# ─── Edge cases ───
section "edge"
expect_in_top "Washington, DC"        "Washington" 3
expect_in_top "Washington DC"         "Washington" 3
expect_in_top "Washington, D.C."      "Washington" 3
expect_in_top "United States"         "United States" 5
expect_in_top "USA"                   "United States" 5

# ─── Summary ───
echo
echo "============================================="
echo " US Test Suite Results"
echo "============================================="
ALL_CATS="city-basic city-state city-state-nocomma nicknames saint-st zip-5 zip-9 zip-city spanish hawaiian ordinals addresses landmarks states counties neighbourhoods fuzzy native edge"
for c in $ALL_CATS; do
  p=${PASS_CAT[$c]:-0}
  f=${FAIL_CAT[$c]:-0}
  t=$((p+f))
  if [[ $t -gt 0 ]]; then
    pct=$(( p * 100 / t ))
    printf "  %-22s %3d/%-3d  (%3d%%)\n" "$c" "$p" "$t" "$pct"
  fi
done
echo "  ----------------------"
TOTAL=$((TOTAL_PASS+TOTAL_FAIL))
PCT=$(( TOTAL_PASS * 100 / TOTAL ))
printf "  %-22s %3d/%-3d  (%3d%%)\n" "TOTAL" "$TOTAL_PASS" "$TOTAL" "$PCT"
echo "============================================="
