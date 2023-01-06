-- BUSINESS_FEATURES_TYPE VIEW
/*
Business Zen Types taxonomy is defined using a hierarchical logic. Many businesses naturally fall into 
multiple categories, for example most steakhouses have a bar but most people would agree that the "primary" classification
of such a business is Restaurant not Bar. 
Google Types are not sorted, and returns ["bar", "restaurant", "food"].
Foursquare returns "Steakhouse" as primary category.
In this case the agreement between foursquare "Steakhouse" and Google Type "restaurant" leads us to select "Restaurant" as Zen Type.

As another example, bowling alley's frequently contain a bar and/or restaurant, but most people would agree that the
"primary" classification is "Bowling Alley".

Google Types are not sorted, and returns ["bar", "restaurant", "food", "bowling_alley"].
Foursquare returns "Bowling Alley" as primary category.

*/
CREATE OR REPLACE MATERIALIZED VIEW BUSINESS_FEATURES_TYPE
comment='Features defining business type / vertical, collected from 3rd party sources combined to create zenreach specific taxonomy' AS
SELECT 
  business_id AS business_id
  , foursquare:primary_category::string AS foursquare_primary_category
  , google:types AS google_types  
  , SPLIT(REPLACE(trim(REGEXP_REPLACE(google_types::string,'("point_of_interest"|"establishment"|\\[|\\]|\\")','',1,0,'i'),','),',,',','),',')[0]::string as first_google_type
  , CASE  
  -- SHOPPING MALL --
      WHEN lower(google:types) LIKE '%shopping_mall%' AND lower(foursquare:primary_category) LIKE '%shopping mall%' THEN 'shopping mall'
      WHEN lower(google:types) LIKE '%shopping_mall%' AND lower(foursquare:primary_category) LIKE '%shopping plaza%' THEN 'shopping mall'

      -- MOVIE THEATER --
      WHEN lower(google:types) LIKE '%movie_theater%' AND lower(foursquare:primary_category) LIKE '%theater%' THEN 'movie theater'

      -- BOWLING ALLEY -- 
      WHEN lower(google:types) LIKE '%bowling_alley%' AND lower(foursquare:primary_category) LIKE '%bowling%' THEN 'bowling alley'

      -- GYM / FITNESS CENTER -- 
      WHEN lower(google:types) LIKE '%gym%' OR lower(foursquare:primary_category) LIKE '%gym%' THEN 'gym / fitness Center'

      -- TRAVEL -- 
      WHEN lower(foursquare:primary_category) LIKE '%airport lounge%' THEN 'travel'

      -- HOTEL / INN / VACATION ACCOMMODATION --
      WHEN lower(google:types) LIKE '%lodging%' and lower(foursquare:primary_category) LIKE '%resort%' THEN 'hotel / inn / vacation accommodation'
      WHEN lower(google:types) LIKE '%lodging%' and lower(foursquare:primary_category) LIKE '%hotel%' THEN 'hotel / inn / vacation accommodation'
      WHEN lower(google:types) LIKE '%lodging%' and lower(foursquare:primary_category) LIKE '%general travel%' THEN 'hotel / inn / vacation accommodation'
      WHEN lower(google:types) LIKE '%lodging%' and lower(foursquare:primary_category) LIKE '%bed & breakfast%' THEN 'hotel / inn / vacation accommodation'
      WHEN lower(google:types) LIKE '%travel_agency%' and lower(foursquare:primary_category) LIKE '%vacation rental%' THEN 'hotel / inn / vacation accommodation'
      WHEN lower(google:types) LIKE '%lodging%' and lower(foursquare:primary_category) LIKE '%vacation rental%' THEN 'hotel / inn / vacation accommodation'

      -- BAR / NIGHTCLUB --
      WHEN lower(google:types) LIKE '%bar%' and lower(foursquare:primary_category) LIKE '%bar%' THEN 'bar / nightclub'
      WHEN lower(google:types) LIKE '%bar%' and lower(foursquare:primary_category) LIKE '%pub%' THEN 'bar / nightclub'
      WHEN lower(google:types) LIKE '%bar%' and lower(foursquare:primary_category) LIKE '%speakeasy%' THEN 'bar / nightclub'
      WHEN lower(google:types) LIKE '%bar%' and lower(foursquare:primary_category) LIKE '%roof deck%' THEN 'bar / nightclub'
      WHEN lower(google:types) LIKE '%bar%' and lower(foursquare:primary_category) LIKE '%lounge%' THEN 'bar / nightclub'
      WHEN lower(google:types) LIKE '%bar%' and lower(foursquare:primary_category) LIKE '%beer%' THEN 'bar / nightclub'
      WHEN lower(google:types) LIKE '%bar%' and lower(foursquare:primary_category) LIKE '%brewery%' THEN 'bar / nightclub'
      WHEN lower(google:types) LIKE '%bar%' and lower(foursquare:primary_category) LIKE '%distillery%' THEN 'bar / nightclub'
      WHEN lower(google:types) LIKE '%bar%' and lower(foursquare:primary_category) LIKE '%rock club%' THEN 'bar / nightclub'
      WHEN lower(google:types) LIKE '%bar%' and lower(foursquare:primary_category) LIKE '%cheese shop%' THEN 'bar / nightclub'
      WHEN lower(google:types) LIKE '%bar%' and lower(foursquare:primary_category) LIKE '%dance club%' THEN 'bar / nightclub'
      WHEN lower(google:types) LIKE '%night_club%' and lower(foursquare:primary_category) LIKE '%jazz club%' THEN 'bar / nightclub'
      WHEN lower(google:types) LIKE '%night_club%' and lower(foursquare:primary_category) LIKE '%jazz club%' THEN 'bar / nightclub'
      WHEN lower(google:types) LIKE '%night_club%' and lower(foursquare:primary_category) LIKE '%jazz club%' THEN 'bar / nightclub'
      WHEN lower(google:types) LIKE '%night_club%' and lower(foursquare:primary_category) LIKE '%dance club%' THEN 'bar / nightclub'
      WHEN lower(google:types) LIKE '%night_club%' and lower(foursquare:primary_category) LIKE '%bar%' THEN 'bar / nightclub'
      WHEN lower(google:types) LIKE '%night_club%' and lower(foursquare:primary_category) LIKE '%lounge%' THEN 'bar / nightclub'
      WHEN lower(google:types) LIKE '%night_club%' and lower(foursquare:primary_category) LIKE '%nightclub%' THEN 'bar / nightclub'
      WHEN lower(google:types) LIKE '%bar%' and lower(google:types) LIKE '%night_club%' and lower(foursquare:primary_category) LIKE 'none' THEN 'bar / nightclub'
      WHEN lower(foursquare:primary_category) LIKE 'bar' THEN  'bar / nightclub'
      WHEN lower(foursquare:primary_category) LIKE 'nightclub' THEN  'bar / nightclub'
      WHEN lower(foursquare:primary_category) LIKE '%brewery%' THEN 'bar / nightclub'
      WHEN lower(foursquare:primary_category) LIKE '%winery%' THEN 'bar / nightclub'
      WHEN lower(foursquare:primary_category) LIKE '%pub%' THEN 'bar / nightclub'
      WHEN lower(foursquare:primary_category) LIKE '%hookah bar%' THEN 'bar / nightclub'
      WHEN lower(foursquare:primary_category) LIKE '%strip club%' THEN 'bar / nightclub'
      WHEN lower(foursquare:primary_category) LIKE 'distillery' THEN  'bar / nightclub'
      WHEN lower(foursquare:primary_category) LIKE 'beach bar' THEN  'bar / nightclub'
      WHEN lower(foursquare:primary_category) LIKE 'beer bar' THEN  'bar / nightclub'
      WHEN lower(foursquare:primary_category) LIKE 'beer garden' THEN  'bar / nightclub'
      WHEN lower(foursquare:primary_category) LIKE '%cocktail bar%' THEN 'bar / nightclub'
      WHEN lower(foursquare:primary_category) LIKE '%wine bar%' THEN 'bar / nightclub'
      WHEN lower(foursquare:primary_category) LIKE '%sports bar%' THEN 'bar / nightclub'
      WHEN lower(foursquare:primary_category) LIKE '%distillery%' THEN 'bar / nightclub'
      WHEN lower(foursquare:primary_category) LIKE '%vineyard%' THEN 'bar / nightclub'

      -- CAFE / BAKERY / DESSERT --
      WHEN lower(google:types) LIKE '%cafe%' and lower(foursquare:primary_category) LIKE '%café%' THEN 'cafe / bakery / dessert'
      WHEN lower(google:types) LIKE '%cafe%' and lower(foursquare:primary_category) LIKE '%coffee%' THEN 'cafe / bakery / dessert'
      WHEN lower(google:types) LIKE '%bakery%' and lower(foursquare:primary_category) LIKE '%bagel%' THEN 'cafe / bakery / dessert'
      WHEN lower(google:types) LIKE '%bakery%' and lower(foursquare:primary_category) LIKE '%bakery%' THEN 'cafe / bakery / dessert'
      WHEN lower(google:types) LIKE '%bakery%' and lower(foursquare:primary_category) LIKE '%donut%' THEN 'cafe / bakery / dessert'
      WHEN lower(google:types) LIKE '%food%' and lower(foursquare:primary_category) LIKE '%donut%' THEN 'cafe / bakery / dessert'
      WHEN lower(google:types) LIKE '%cafe%' and lower(foursquare:primary_category) LIKE '%bistro%' THEN 'cafe / bakery / dessert'
      WHEN lower(google:types) LIKE '%cafe%' and lower(foursquare:primary_category) LIKE '%bubble tea shop%' THEN 'cafe / bakery / dessert'
      WHEN lower(google:types) LIKE '%cafe%' and lower(foursquare:primary_category) LIKE '%bakery%' THEN 'cafe / bakery / dessert'
      WHEN lower(google:types) LIKE '%cafe%' and lower(foursquare:primary_category) LIKE '%pastry shop%' THEN 'cafe / bakery / dessert'
      WHEN lower(google:types) LIKE '%bakery%' and lower(foursquare:primary_category) LIKE '%cupcake shop%' THEN 'cafe / bakery / dessert'
      WHEN lower(google:types) LIKE '%bakery%' and lower(foursquare:primary_category) LIKE '%pie shop%' THEN 'cafe / bakery / dessert'
      WHEN lower(google:types) LIKE '%food%' and lower(foursquare:primary_category) LIKE '%cupcake shop%' THEN 'cafe / bakery / dessert'
      WHEN lower(google:types) LIKE '%food%' and lower(foursquare:primary_category) LIKE '%ice cream shop%' THEN 'cafe / bakery / dessert'
      WHEN lower(foursquare:primary_category) LIKE '%bagel%' THEN 'cafe / bakery / dessert'
      WHEN lower(foursquare:primary_category) LIKE '%café%' THEN 'cafe / bakery / dessert'
      WHEN lower(foursquare:primary_category) LIKE '%coffee shop%' THEN 'cafe / bakery / dessert'
      WHEN lower(foursquare:primary_category) LIKE '%creperie%' THEN 'cafe / bakery / dessert'
      WHEN lower(foursquare:primary_category) LIKE '%dessert%' THEN 'cafe / bakery / dessert'
      WHEN lower(foursquare:primary_category) LIKE '%tea room%' THEN 'cafe / bakery / dessert'
      WHEN lower(foursquare:primary_category) LIKE '%bubble tea shop%' THEN 'cafe / bakery / dessert'
      WHEN lower(foursquare:primary_category) LIKE '%bakery%' THEN 'cafe / bakery / dessert'
      WHEN lower(foursquare:primary_category) LIKE '%frozen yogurt shop%' THEN 'cafe / bakery / dessert'
      WHEN lower(foursquare:primary_category) LIKE '%pie shop%' THEN 'cafe / bakery / dessert'

      -- RESTAURANT -- 
      WHEN lower(google:types) LIKE '%restaurant%' and lower(foursquare:primary_category) LIKE '%restaurant%' THEN 'restaurant'
      WHEN lower(google:types) LIKE '%restaurant%' and lower(foursquare:primary_category) LIKE '%steakhouse%' THEN 'restaurant'
      WHEN lower(google:types) LIKE '%restaurant%' and lower(foursquare:primary_category) LIKE '%pizza%' THEN 'restaurant'
      WHEN lower(google:types) LIKE '%restaurant%' and lower(foursquare:primary_category) LIKE '%chaat%' THEN 'restaurant'
      WHEN lower(google:types) LIKE '%restaurant%' and lower(foursquare:primary_category) LIKE '%cafeteria%' THEN 'restaurant'
      WHEN lower(google:types) LIKE '%restaurant%' and lower(foursquare:primary_category) LIKE '%cafeteria%' THEN 'restaurant'
      WHEN lower(google:types) LIKE '%restaurant%' and lower(foursquare:primary_category) LIKE '%burger%' THEN 'restaurant'
      WHEN lower(google:types) LIKE '%restaurant%' and lower(foursquare:primary_category) LIKE '%breakfast%' THEN 'restaurant'
      WHEN lower(google:types) LIKE '%restaurant%' and lower(foursquare:primary_category) LIKE '%poke%' THEN 'restaurant'
      WHEN lower(google:types) LIKE '%restaurant%' and lower(foursquare:primary_category) LIKE '%noodle%' THEN 'restaurant'
      WHEN lower(google:types) LIKE '%restaurant%' and lower(foursquare:primary_category) LIKE '%diner%' THEN 'restaurant'
      WHEN lower(google:types) LIKE '%restaurant%' and lower(foursquare:primary_category) LIKE '%bbq%' THEN 'restaurant'
      WHEN lower(google:types) LIKE '%restaurant%' and lower(foursquare:primary_category) LIKE '%wings%' THEN 'restaurant'
      WHEN lower(google:types) LIKE '%restaurant%' and lower(foursquare:primary_category) LIKE '%hot dog%' THEN 'restaurant'
      WHEN lower(google:types) LIKE '%restaurant%' and lower(foursquare:primary_category) LIKE '%buffet%' THEN 'restaurant'
      WHEN lower(google:types) LIKE '%restaurant%' and lower(foursquare:primary_category) LIKE '%soup%' THEN 'restaurant'
      WHEN lower(google:types) LIKE '%restaurant%' and lower(foursquare:primary_category) LIKE '%salad%' THEN 'restaurant'
      WHEN lower(google:types) LIKE '%restaurant%' and lower(foursquare:primary_category) LIKE '%sandwich place%' THEN 'restaurant'
      WHEN lower(google:types) LIKE '%restaurant%' and lower(foursquare:primary_category) LIKE '%taco%' THEN 'restaurant'
      WHEN lower(google:types) LIKE '%restaurant%' and lower(foursquare:primary_category) LIKE '%juice%' THEN 'restaurant'
      WHEN lower(google:types) LIKE '%restaurant%' and lower(foursquare:primary_category) LIKE '%burrito%' THEN 'restaurant'
      WHEN lower(google:types) LIKE '%restaurant%' and lower(foursquare:primary_category) LIKE '%bistro%' THEN 'restaurant'
      WHEN lower(google:types) LIKE '%restaurant%' and lower(foursquare:primary_category) LIKE '%mac & cheese%' THEN 'restaurant'
      WHEN lower(google:types) LIKE '%restaurant%' and lower(foursquare:primary_category) LIKE '%deli / bodega%' THEN 'restaurant'
      WHEN lower(google:types) LIKE '%restaurant%' and lower(foursquare:primary_category) LIKE '%fried chicken%' THEN 'restaurant'
      WHEN lower(google:types) LIKE '%restaurant%' and lower(foursquare:primary_category) LIKE '%brasserie%' THEN 'restaurant'
      WHEN lower(google:types) LIKE '%restaurant%' and lower(foursquare:primary_category) LIKE '%lounge%' THEN 'restaurant'
      WHEN lower(google:types) LIKE '%restaurant%' and lower(foursquare:primary_category) LIKE '%lounge%' THEN 'restaurant'
      WHEN lower(foursquare:primary_category) LIKE '%restaurant%' THEN 'restaurant'
      WHEN lower(foursquare:primary_category) LIKE '%food court%' THEN 'restaurant'
      WHEN lower(foursquare:primary_category) LIKE '%steakhouse%' THEN 'restaurant'
      WHEN lower(foursquare:primary_category) LIKE '%juice bar%' THEN 'restaurant'
      WHEN lower(foursquare:primary_category) LIKE '%bbq joint%' THEN 'restaurant'
      WHEN lower(foursquare:primary_category) LIKE '%burger joint%' THEN 'restaurant'
      WHEN lower(foursquare:primary_category) LIKE '%pizza place%' THEN 'restaurant'
      WHEN lower(foursquare:primary_category) LIKE '%sandwich place%' THEN 'restaurant'
      WHEN lower(foursquare:primary_category) LIKE '%breakfast spot%' THEN 'restaurant'

      -- MEDICAL -- 
      WHEN lower(google:types) LIKE '%doctor%' and lower(foursquare:primary_category) LIKE '%doctor%' THEN 'medical'
      WHEN lower(google:types) LIKE '%dentist%' and lower(foursquare:primary_category) LIKE '%dentist%' THEN 'medical'
      WHEN lower(google:types) LIKE '%health%' and lower(foursquare:primary_category) LIKE '%eye doctor%' THEN 'medical'      
      WHEN lower(google:types) LIKE '%health%' and lower(foursquare:primary_category) LIKE '%doctor%' THEN 'medical'
      WHEN lower(google:types) LIKE '%health%' and lower(foursquare:primary_category) LIKE '%medical center%' THEN 'medical'
      WHEN lower(google:types) LIKE '%doctor%' and lower(foursquare:primary_category) LIKE '%emergency room%' THEN 'medical'
      WHEN lower(google:types) LIKE '%pharmacy%' and lower(foursquare:primary_category) LIKE '%pharmacy%' THEN 'medical'
      WHEN lower(google:types) LIKE '%health%' and lower(foursquare:primary_category) LIKE '%pharmacy%' THEN 'medical'
      WHEN lower(foursquare:primary_category) LIKE '%emergency room%' THEN 'medical'

      -- SPA / SALON --
      WHEN lower(google:types) LIKE '%beauty_salon%' and lower(foursquare:primary_category) LIKE '%salon%' THEN 'spa / salon'
      WHEN lower(google:types) LIKE '%beauty_salon%' and lower(foursquare:primary_category) LIKE '%barbershop%' THEN 'spa / salon' 
      WHEN lower(google:types) LIKE '%hair_care%' and lower(foursquare:primary_category) LIKE '%barbershop%' THEN 'spa / salon'
      WHEN lower(google:types) LIKE '%spa%' and lower(foursquare:primary_category) LIKE '%spa%' THEN 'spa / salon'
      WHEN lower(foursquare:primary_category) LIKE '%health & beauty service%' THEN 'spa / salon'
      WHEN lower(foursquare:primary_category) LIKE '%salon%' THEN 'spa / salon'
      WHEN lower(foursquare:primary_category) LIKE 'spa' THEN 'spa / salon'

      -- UNIVERSITY --
      WHEN lower(google:types) LIKE '%university%' and lower(foursquare:primary_category) LIKE '%school%' THEN 'university'

      -- PRESCHOOL --
      WHEN lower(google:types) LIKE '%school%' and lower(foursquare:primary_category) LIKE '%preschool%' THEN 'preschool'

      -- LAUNDROMAT / LAUNDRY SERVICE --
      WHEN lower(google:types) LIKE '%laundry%' THEN 'laundromat / laundry service'
      WHEN lower(foursquare:primary_category) LIKE '%laundry%' THEN 'laundromat / laundry service'  

      -- AUTOMOTIVE --
      WHEN lower(google:types) LIKE '%car_wash%' THEN 'automotive'
      WHEN lower(google:types) LIKE '%car_repair%' THEN 'automotive'
      WHEN lower(google:types) LIKE '%car_dealer%' THEN 'automotive'
      WHEN lower(google:types) LIKE '%gas_station%' THEN 'automotive'
      WHEN lower(foursquare:primary_category) LIKE '%car wash%' THEN 'automotive'
      WHEN lower(foursquare:primary_category) LIKE '%automotive shop%' THEN  'automotive'

      -- LAWYER --
      WHEN lower(google:types) LIKE '%lawyer%' and lower(foursquare:primary_category) LIKE '%lawyer%' THEN 'lawyer'

      -- RETAIL --
      WHEN lower(google:types) LIKE '%store%' and lower(foursquare:primary_category) LIKE '%bike shop%' THEN 'retail'
      WHEN lower(google:types) LIKE '%store%' and lower(foursquare:primary_category) LIKE '%board shop%' THEN 'retail'
      WHEN lower(google:types) LIKE '%store%' and lower(foursquare:primary_category) LIKE '%beer store%' THEN 'retail'
      WHEN lower(google:types) LIKE '%store%' and lower(foursquare:primary_category) LIKE '%hunting supply%' THEN 'retail'
      WHEN lower(google:types) LIKE '%store%' and lower(foursquare:primary_category) LIKE '%liquor store%' THEN 'retail'
      WHEN lower(google:types) LIKE '%store%' and lower(foursquare:primary_category) LIKE '%smoke shop%' THEN 'retail'
      WHEN lower(google:types) LIKE '%store%' and lower(foursquare:primary_category) LIKE '%hardware store%' THEN 'retail'
      WHEN lower(google:types) LIKE '%clothing_store%' and lower(foursquare:primary_category) LIKE '%boutique%' THEN 'retail'
      WHEN lower(google:types) LIKE '%clothing_store%' and lower(foursquare:primary_category) LIKE '%clothing store%' THEN 'retail'
      WHEN lower(google:types) LIKE '%grocery_or_supermarket%' and lower(foursquare:primary_category) LIKE '%market%' THEN 'retail'
      WHEN lower(google:types) LIKE '%grocery_or_supermarket%' and lower(foursquare:primary_category) LIKE '%supermarket%' THEN 'retail'
      WHEN lower(google:types) LIKE '%grocery_or_supermarket%' and lower(foursquare:primary_category) LIKE '%grocery store%' THEN 'retail'
      WHEN lower(google:types) LIKE '%supermarket%' and lower(foursquare:primary_category) LIKE '%supermarket%' THEN 'retail'
      WHEN lower(google:types) LIKE '%store%' and lower(foursquare:primary_category) LIKE '%butcher%' THEN 'retail'
      WHEN lower(google:types) LIKE '%store%' and lower(foursquare:primary_category) LIKE '%candy store%' THEN 'retail'
      WHEN lower(google:types) LIKE '%store%' and lower(foursquare:primary_category) LIKE '%jewelry store%' THEN 'retail'
      WHEN lower(google:types) LIKE '%store%' and lower(foursquare:primary_category) LIKE '%toy / game store%' THEN 'retail'
      WHEN lower(google:types) LIKE '%store%' and lower(foursquare:primary_category) LIKE '%cheese shop%' THEN 'retail'
      WHEN lower(google:types) LIKE '%store%' and lower(foursquare:primary_category) LIKE '%cosmetics shop%' THEN 'retail'
      WHEN lower(google:types) LIKE '%store%' and lower(foursquare:primary_category) LIKE '%flower shop%' THEN 'retail'
      WHEN lower(google:types) LIKE '%store%' and lower(foursquare:primary_category) LIKE '%gourmet shop%' THEN 'retail'
      WHEN lower(google:types) LIKE '%store%' and lower(foursquare:primary_category) LIKE '%miscellaneous shop%' THEN 'retail'
      WHEN lower(google:types) LIKE '%store%' and lower(foursquare:primary_category) LIKE '%shoe store%' THEN 'retail'
      WHEN lower(google:types) LIKE '%store%' and lower(foursquare:primary_category) LIKE '%sporting goods shop%' THEN 'retail'
      WHEN lower(google:types) LIKE '%store%' and lower(foursquare:primary_category) LIKE '%vape store%' THEN 'retail'
      WHEN lower(google:types) LIKE '%store%' and lower(foursquare:primary_category) LIKE '%video game store%' THEN 'retail'
      WHEN lower(google:types) LIKE '%store%' and lower(foursquare:primary_category) LIKE '%wine shop%' THEN 'retail'
      WHEN lower(google:types) LIKE '%store%' and lower(foursquare:primary_category) LIKE '%women\'s store%' THEN 'retail'
      WHEN lower(google:types) LIKE '%store%' and lower(foursquare:primary_category) LIKE '%mobile phone shop%' THEN 'retail'
      WHEN lower(google:types) LIKE '%store%' and lower(foursquare:primary_category) LIKE '%chocolate shop%' THEN 'retail'
      WHEN lower(google:types) LIKE '%store%' and lower(foursquare:primary_category) LIKE '%department store%' THEN 'retail'
      WHEN lower(google:types) LIKE '%store%' and lower(foursquare:primary_category) LIKE '%florist%' THEN 'retail'
      WHEN lower(google:types) LIKE '%store%' and lower(foursquare:primary_category) LIKE '%knitting store%' THEN 'retail'
      WHEN lower(foursquare:primary_category) LIKE '%arts & crafts store%' THEN 'retail'
      WHEN lower(foursquare:primary_category) LIKE '%furniture / home store%' THEN 'retail'
      WHEN lower(foursquare:primary_category) LIKE '%antique shop%' THEN 'retail'
      -- PLACE OF WORSHIP --
      WHEN lower(google:types) LIKE '%church%'then 'place of worship'
      WHEN lower(google:types) LIKE '%place_of_worship%' THEN 'place of worship'
      WHEN lower(foursquare:primary_category) LIKE '%church%' THEN 'place of worship'
      -- FUNERAL HOME --
      WHEN lower(google:types) LIKE '%funeral_home%' THEN 'funeral home'
      -- CASINO --
      WHEN lower(google:types) LIKE '%casino%' THEN 'casino'
      WHEN lower(foursquare:primary_category) LIKE '%casino%' THEN 'casino'
      -- MARKETING / IT --
      WHEN lower(foursquare:primary_category) LIKE '%it services%' THEN 'marketing / it / business services'
      WHEN lower(foursquare:primary_category) LIKE '%advertising agency%' THEN 'marketing / it / business services'
      WHEN lower(foursquare:primary_category) LIKE '%tech startup%' THEN 'marketing / it / business services'
      WHEN lower(foursquare:primary_category) LIKE '%design studio%' THEN 'marketing / it / business services'
      WHEN lower(foursquare:primary_category) LIKE '%business service%' THEN 'marketing / it / business services'
      -- SPORTS STADIUM --
      WHEN lower(foursquare:primary_category) LIKE '%baseball stadium%' THEN 'sports stadium'
      -- SPORTS CENTER / OUTDOOR SPORT
      WHEN lower(foursquare:primary_category) LIKE '%baseball field%' THEN 'sport center / outdoor sports'
      WHEN lower(foursquare:primary_category) LIKE '%skate park%' THEN 'sport center / outdoor sports'
      WHEN lower(foursquare:primary_category) LIKE '%skating rink%' THEN 'sport center / outdoor sports'
      WHEN lower(foursquare:primary_category) LIKE '%ski%' THEN 'sport center / outdoor sports'
      WHEN lower(foursquare:primary_category) LIKE '%hockey arena%' THEN 'sport center / outdoor sports'
      WHEN lower(foursquare:primary_category) LIKE '%water park%' THEN 'sport center / outdoor sports'
      WHEN lower(foursquare:primary_category) LIKE '%harbor / marina%' THEN 'sport center / outdoor sports'
      WHEN lower(foursquare:primary_category) LIKE '%boat rental%' THEN 'sport center / outdoor sports'
      WHEN lower(foursquare:primary_category) LIKE '%roller rink%' THEN 'sport center / outdoor sports'
      WHEN lower(foursquare:primary_category) LIKE '%sports club%' THEN 'sport center / outdoor sports'
      -- GOLF COURSE --
      WHEN lower(foursquare:primary_category) LIKE '%golf course%' THEN 'golf'
      WHEN lower(foursquare:primary_category) LIKE '%golf driving range%' THEN 'golf'
      -- GUN RANGE --
      WHEN lower(foursquare:primary_category) LIKE '%gun range%' THEN 'gun range'
      -- GALLERY / MUSEUM --
      WHEN lower(foursquare:primary_category) LIKE '%art gallery%' THEN 'gallery / museum'
      WHEN lower(foursquare:primary_category) LIKE '%art museum%' THEN  'gallery / museum'
      WHEN lower(foursquare:primary_category) LIKE '%museum%' THEN  'gallery / museum'
      -- EVENT SPACE -- 
      WHEN lower(foursquare:primary_category) LIKE '%event space%' THEN lower(foursquare:primary_category)
      -- PERFORMANCE VENUE -- 
      WHEN lower(foursquare:primary_category) LIKE '%music venue%' THEN 'performance venue'     
      WHEN lower(foursquare:primary_category) LIKE '%comedy club%' THEN 'performance venue' 
      WHEN lower(foursquare:primary_category) LIKE '%performing arts venue%' THEN 'performance venue' 
      WHEN lower(foursquare:primary_category) LIKE '%piano bar%' THEN 'performance venue' 
      WHEN lower(foursquare:primary_category) LIKE '%theater%' THEN 'performance venue'
      -- TOURISM 
      WHEN lower(foursquare:primary_category) LIKE '%tour provider%' THEN 'tourist attraction'
      WHEN lower(google:types) LIKE '%travel_agency%' and lower(foursquare:primary_category) LIKE '%heliport%' THEN 'tourist attraction'
      -- THE REST
      WHEN lower(foursquare:primary_category) LIKE '%farm%' THEN lower(foursquare:primary_category)
      WHEN lower(foursquare:primary_category) LIKE '%theme park%' THEN lower(foursquare:primary_category)      
      WHEN lower(foursquare:primary_category) LIKE '%other great outdoors%' THEN lower(foursquare:primary_category)
      WHEN lower(foursquare:primary_category) LIKE '%rv park%' THEN lower(foursquare:primary_category)
      WHEN lower(foursquare:primary_category) LIKE '%beach%' THEN lower(foursquare:primary_category)
      WHEN lower(foursquare:primary_category) LIKE '%science museum%' THEN lower(foursquare:primary_category)
      WHEN lower(foursquare:primary_category) LIKE '%go kart track%' THEN lower(foursquare:primary_category)
      WHEN lower(foursquare:primary_category) LIKE '%gaming cafe%' THEN lower(foursquare:primary_category)
      WHEN lower(foursquare:primary_category) LIKE '%laser tag%' THEN lower(foursquare:primary_category)
      WHEN lower(foursquare:primary_category) LIKE '%general entertainment%' THEN lower(foursquare:primary_category)
      WHEN lower(foursquare:primary_category) LIKE '%mini golf%' THEN lower(foursquare:primary_category)
      WHEN lower(foursquare:primary_category) LIKE '%arcade%' THEN 'arcade'
      WHEN lower(foursquare:primary_category) LIKE '%playground%' THEN lower(foursquare:primary_category)
      WHEN lower(foursquare:primary_category) LIKE '%summer camp%' THEN lower(foursquare:primary_category)
      WHEN lower(foursquare:primary_category) LIKE '%pool hall%' THEN lower(foursquare:primary_category)
      WHEN lower(foursquare:primary_category) LIKE '%recreation center%' THEN lower(foursquare:primary_category)
      WHEN lower(foursquare:primary_category) LIKE '%cultural center%' THEN lower(foursquare:primary_category)
      WHEN lower(foursquare:primary_category) LIKE '%office%' THEN lower(foursquare:primary_category)
      WHEN lower(foursquare:primary_category) LIKE '%coworking space%' THEN lower(foursquare:primary_category)
      WHEN lower(foursquare:primary_category) LIKE '%food service%' THEN lower(foursquare:primary_category)
      WHEN lower(foursquare:primary_category) LIKE '%marijuana dispensary%' THEN lower(foursquare:primary_category)
      WHEN lower(foursquare:primary_category) LIKE '%apartment / condo%' THEN lower(foursquare:primary_category)
      WHEN lower(foursquare:primary_category) LIKE '%recording studio%' THEN lower(foursquare:primary_category)
      -- LAST FALL BACK TO CLASSIFICATION BY GOOGLE TYPES
      WHEN lower(google:types) LIKE '%restaurant%' THEN 'restaurant'
      WHEN lower(google:types) LIKE '%bar%' THEN 'bar / nightclub'
      WHEN lower(google:types) LIKE '%night_club%' THEN 'bar / nightclub'
      WHEN lower(google:types) LIKE '%cafe%' THEN 'cafe / bakery / dessert'
      WHEN lower(google:types) LIKE '%bakery%' THEN 'cafe / bakery / dessert'
      WHEN lower(google:types) LIKE '%spa%' THEN 'spa / salon'
      WHEN lower(google:types) LIKE '%doctor%' THEN 'medical'
      WHEN lower(google:types) LIKE '%dentist%' THEN 'medical'
      WHEN lower(google:types) LIKE '%grocery_store%' THEN 'retail'
      WHEN lower(google:types) LIKE '%furniture_store%' THEN 'retail'
      WHEN lower(google:types) LIKE '%convenience_store%' THEN 'retail'
      WHEN lower(google:types) LIKE '%electronics_store%' THEN 'retail'
      WHEN lower(google:types) LIKE '%park%' THEN 'park'
      ELSE NULL
 END AS zen_type
FROM "BUSINESS_FEATURES_RAW"
WHERE valid_rec = TRUE AND processed = TRUE and manual_review = False;