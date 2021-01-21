/**
 * @file schema.hpp
 * @brief defines Schema of each relation.
 *
 * https://www.cs.umb.edu/~xuedchen/research/publications/StarSchemaB.PDF
 *
 *
 * Each Schema-struct consists of:
 *
 * - id: type_id of relation
 * - Key-struct: columns for key of the relation
 * - List of values: other columns
 * - foldKey(): writes key compressed to writer, returns length of compressed object
 * - unfoldKey(): reads key and decompresses it from input, returns length of compressed object
 * - maxFoldLength(): max length of compressed key
 *
 */

struct lineorder_t {
   /*
    * SF*6,000,000 are populated
    * As in TPC-H, Orders are not present for all customers.
    * The orders are assigned at random to two-thirds of the customers.
    */
   static constexpr int id = 0;
   struct Key {
      static constexpr int id = 0;
      /*
       * ORDERKEY numeric (int up to SF 300)  first 8 of each 32 keys used
       * LINENUMBER numeric 1-7
       */
      Integer order_key;
      Integer line_number;
   };
   /*
    * CUSTKEY numeric identifier foreign key reference to C_CUSTKEY
    * PARTKEY identifier foreign key reference to P_PARTKEY
    * SUPPKEY numeric identifier foreign key reference to S_SUPPKEY
    * ORDERDATE identifier foreign key reference to D_DATEKEY
    * ORDERPRIORITY fixed text, size 15 (5 Priorities: 1-URGENT, etc.)
    * SHIPPRIORITY fixed text, size 1
    * QUANTITY numeric 1-50 (for PART)
    * EXTENDEDPRICE numeric, MAX about 55,450 (for PART)
    * ORDTOTALPRICE numeric, MAX about 388,000 (for ORDER)
    * DISCOUNT numeric 0-10 (for PART) -- (Represents PERCENT)
    * REVENUE numeric (for PART: (extendedprice*(100-discount))/100)
    * SUPPLYCOST numeric (for PART, cost from supplier, max = ?)
    * TAX numeric 0-8 (for PART)
    * COMMITDATE Foreign Key reference to D_DATEKEY
    * SHIPMODE fixed text, size 10 (Modes: REG AIR, AIR, etc.)
    */
   Integer cust_key;
   Integer part_key;
   Integer supp_key;
   Integer oder_date;
   Varchar<15> order_priority;
   Varchar<1> ship_priority;
   Integer quantity;
   Numeric extended_price;
   Numeric order_total_price;
   Integer discount;
   Numeric revenue;
   Numeric supply_cost;
   Integer tax;
   Integer commit_date;
   Varchar<10> ship_mode;
   // -------------------------------------------------------------------------------------
   template <class T>
   static unsigned foldKey(uint8_t* out, const T& record)
   {
      unsigned pos = 0;
      pos += fold(out + pos, record.order_key);
      pos += fold(out + pos, record.line_number);
      return pos;
   }
   template <class T>
   static unsigned unfoldKey(const uint8_t* in, T& record)
   {
      unsigned pos = 0;
      pos += fold(out + pos, record.order_key);
      pos += fold(out + pos, record.line_number);
      return pos;
   }
   static constexpr unsigned maxFoldLength() { return 0 + sizeof(Key::order_key) + sizeof(Key::line_number); };
};

struct part_t {
   //  200,000*floor(1+log_2 SF) populated
   static constexpr int id = 1;
   struct Key {
      static constexpr int id = 1;
      /*
       * PARTKEY identifier
       */
      Integer part_key;
   };
   /*
    * NAME variable text, size 22 (Not unique per PART but never was)
    * MFGR fixed text, size 6 (MFGR#1-5, CARD = 5)
    * CATEGORY fixed text, size 7 ('MFGR#'||1-5||1-5: CARD = 25)
    * BRAND1 fixed text, size 9 (CATEGORY||1-40: CARD = 1000)
    * COLOR variable text, size 11 (CARD = 94)
    * TYPE variable text, size 25 (CARD = 150)
    * SIZE numeric 1-50 (CARD = 50)
    * CONTAINER fixed text(10) (CARD = 40)
    */
   Varchar<22> namePart;
   Varchar<6> mfgr;
   Varchar<8> category;
   Varchar<9> brand1;
   Varchar<11> color;
   Varchar<25> type;
   Integer size;
   Varchar<10> container;

   template <class T>
   static unsigned foldKey(uint8_t* out, const T& record)
   {
      unsigned pos = 0;
      pos += fold(out + pos, record.part_key);
      return pos;
   }
   template <class T>
   static unsigned unfoldKey(const uint8_t* in, T& record)
   {
      unsigned pos = 0;
      pos += unfold(in + pos, record.part_key);
      return pos;
   }
   static constexpr unsigned maxFoldLength() { return 0 + sizeof(Key::part_key); };
};

struct supplier_t {
   //  SF*10,000 are populated
   static constexpr int id = 2;
   struct Key {
      static constexpr int id = 2;
      /*
       * SUPPKEY identifier
       */
      Integer supp_key;
   };
   /*
    * NAME fixed text, size 25: 'Supplier'||SUPPKEY
    * ADDRESS variable text, size 25 (city below)
    * CITY fixed text, size 10 (10/nation: nation_prefix||(0-9))
    * NATION fixed text(15) (25 values, longest UNITED KINGDOM)
    * REGION fixed text, size 12 (5 values: longest MIDDLE EAST)
    * PHONE fixed text, size 15 (many values, format: 43-617-354-1222)
    */
   Varchar<25> name;
   Varchar<25> address;
   Varchar<10> city;
   Varchar<15> nation;
   Varchar<12> region;
   Varchar<15> phone;

   template <class T>
   static unsigned foldKey(uint8_t* out, const T& record)
   {
      unsigned pos = 0;
      pos += fold(out + pos, record.supp_key);
      return pos;
   }
   template <class T>
   static unsigned unfoldKey(const uint8_t* in, T& record)
   {
      unsigned pos = 0;
      pos += unfold(in + pos, record.supp_key);
      return pos;
   }
   static constexpr unsigned maxFoldLength() { return 0 + sizeof(Key::supp_key); };
};

struct customer_t {
   //  SF*30,000 are populated
   static constexpr int id = 3;
   struct Key {
      static constexpr int id = 3;
      /*
       * CUSTKEY identifier
       */
      Integer cust_key;
   };
   /*
    * NAME variable text, size 25 'Customer'||CUSTKEY
    * ADDRESS variable text, size 25 (city below)
    * CITY fixed text, size 10 (10/nation: NATION_PREFIX||(0-9)
    * NATION fixed text(15) (25 values, longest UNITED KINGDOM)
    * REGION fixed text, size 12 (5 values: longest MIDDLE EAST)
    * PHONE fixed text, size 15 (many values, format: 43-617-354-1222)
    * MKTSEGMENT fixed text, size 10 (longest is AUTOMOBILE)
    */
   Varchar<25> name;
   Varchar<25> address;
   Varchar<10> city;
   Varchar<15> nation;
   Varchar<12> region;
   Varchar<15> phone;
   Varchar<10> mkt_segment;

   template <class T>
   static unsigned foldKey(uint8_t* out, const T& record)
   {
      unsigned pos = 0;
      pos += fold(out + pos, record.cust_key);
      return pos;
   }
   template <class T>
   static unsigned unfoldKey(const uint8_t* in, T& record)
   {
      unsigned pos = 0;
      pos += unfold(in + pos, record.cust_key);
      return pos;
   }
   static constexpr unsigned maxFoldLength() { return 0 + sizeof(Key::cust_key); };
};

struct date_t {
   // 7 years of days; 7366 days
   static constexpr int id = 4;
   struct Key {
      static constexpr int id = 4;
      /*
       * DATEKEY identifier, unique id -- e.g. 19980327 (what we use)
       */
      Integer date_key;
   };
   /*
    * DATE fixed text, size 18, longest: December 22, 1998
    * DAYOFWEEK fixed text, size 8, Sunday, Monday, ..., Saturday)
    * MONTH fixed text, size 9: January, ..., December
    * YEAR unique value 1992-1998
    * YEARMONTHNUM numeric (YYYYMM) -- e.g. 199803
    * YEARMONTH fixed text, size 7: Mar1998 for example
    * DAYNUMINWEEK numeric 1-7
    * DAYNUMINMONTH numeric 1-31
    * DAYNUMINYEAR numeric 1-366
    * MONTHNUMINYEAR numeric 1-12
    * WEEKNUMINYEAR numeric 1-53
    * SELLINGSEASON text, size 12 (Christmas, Summer,...)
    * LASTDAYINWEEKFL 1 bit
    * LASTDAYINMONTHFL 1 bit
    * HOLIDAYFL 1 bit
    * WEEKDAYFL 1 bit
    */
   Varchar<18> date;
   Varchar<8> day_of_week;
   Varchar<9> month;
   Integer year;
   Integer year_month_number;
   Varchar<7> year_month;
   Integer day_num_in_week;
   Integer day_num_in_month;
   Integer day_num_in_year;
   Integer month_num_in_year;
   Integer week_num_in_year;
   Varchar<12> selling_season;
   Integer last_day_in_week_fl;   // 1 bit
   Integer last_day_in_month_fl;  // 1 bit
   Integer holiday_fl;            //  1 bit
   Integer weekday_fl;            //  1 bit

   template <class T>
   static unsigned foldKey(uint8_t* out, const T& record)
   {
      unsigned pos = 0;
      pos += fold(out + pos, record.date_key);
      return pos;
   }
   template <class T>
   static unsigned unfoldKey(const uint8_t* in, T& record)
   {
      unsigned pos = 0;
      pos += unfold(in + pos, record.date_key);
      return pos;
   }
   static constexpr unsigned maxFoldLength() { return 0 + sizeof(Key::date_key); };
};
