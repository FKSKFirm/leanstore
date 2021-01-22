/**
 * @file schema.hpp
 * @brief defines Schema of each relation.
 *
 * 2009 Description:
 * https://www.cs.umb.edu/~poneil/StarSchemaB.pdf
 * 2007 Description:
 * https://www.cs.umb.edu/~xuedchen/research/publications/StarSchemaB.PDF
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
#include "../shared/types.hpp"

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
       * LO_ORDERKEY numeric (int up to SF 300) first 8 of each 32 keys populated
       * LO_LINENUMBER numeric 1-7
       */
      Integer lo_order_key;
      Integer lo_line_number;
   };
   /*
    * LO_CUSTKEY numeric identifier FK to C_CUSTKEY
    * LO_PARTKEY identifier FK to P_PARTKEY
    * LO_SUPPKEY numeric identifier FK to S_SUPPKEY
    * LO_ORDERDATE identifier FK to D_DATEKEY
    * LO_ORDERPRIORITY fixed text, size 15 (See pg 91: 5 Priorities: 1-URGENT, etc.)
    * LO_SHIPPRIORITY fixed text, size 1
    * LO_QUANTITY numeric 1-50 (for PART)
    * LO_EXTENDEDPRICE numeric ≤ 55,450 (for PART)
    * LO_ORDTOTALPRICE numeric ≤ 388,000 (ORDER)
    * LO_DISCOUNT numeric 0-10 (for PART, percent)
    * LO_REVENUE numeric (for PART: (lo_extendedprice*(100-lo_discnt))/100)
    * LO_SUPPLYCOST numeric (for PART)
    * LO_TAX numeric 0-8 (for PART)
    * LO_COMMITDATE FK to D_DATEKEY
    * LO_SHIPMODE fixed text, size 10 (See pg. 91: 7 Modes: REG AIR, AIR, etc.)
    */
   Integer lo_cust_key;
   Integer lo_part_key;
   Integer lo_supp_key;
   Integer lo_oder_date;
   Varchar<15> lo_order_priority;
   Varchar<1> lo_ship_priority;
   Integer lo_quantity;
   Numeric lo_extended_price;
   Numeric lo_order_total_price;
   Integer lo_discount;
   Numeric lo_revenue;
   Numeric lo_supply_cost;
   Integer lo_tax;
   Integer lo_commit_date;
   Varchar<10> lo_ship_mode;
   // -------------------------------------------------------------------------------------
   template <class T>
   static unsigned foldKey(uint8_t* out, const T& record)
   {
      unsigned pos = 0;
      pos += fold(out + pos, record.lo_order_key);
      pos += fold(out + pos, record.lo_line_number);
      return pos;
   }
   template <class T>
   static unsigned unfoldKey(const uint8_t* in, T& record)
   {
      unsigned pos = 0;
      pos += unfold(in + pos, record.lo_order_key);
      pos += unfold(in + pos, record.lo_line_number);
      return pos;
   }
   static constexpr unsigned maxFoldLength() { return 0 + sizeof(Key::lo_order_key) + sizeof(Key::lo_line_number); };
};

struct part_t {
   //  200,000*floor(1+log_2 SF) populated
   static constexpr int id = 1;
   struct Key {
      static constexpr int id = 1;
      /*
       * P_PARTKEY identifier
       */
      Integer p_part_key;
   };
   /*
    * P_NAME variable text, size 22 (Not unique)
    * P_MFGR fixed text, size 6 (MFGR#1-5, CARD = 5)
    * P_CATEGORY fixed text, size 7 ('MFGR#'||1-5||1-5: CARD = 25)
    * P_BRAND1 fixed text, size 9 (P_CATEGORY||1-40: CARD = 1000)
    * P_COLOR variable text, size 11 (CARD = 94)
    * P_TYPE variable text, size 25 (CARD = 150)
    * P_SIZE numeric 1-50 (CARD = 50)
    * P_CONTAINER fixed text, size 10 (CARD = 40)
    */
   Varchar<22> p_namePart;
   Varchar<6> p_mfgr;
   Varchar<8> p_category;
   Varchar<9> p_brand1;
   Varchar<11> p_color;
   Varchar<25> p_type;
   Integer p_size;
   Varchar<10> p_container;

   template <class T>
   static unsigned foldKey(uint8_t* out, const T& record)
   {
      unsigned pos = 0;
      pos += fold(out + pos, record.p_part_key);
      return pos;
   }
   template <class T>
   static unsigned unfoldKey(const uint8_t* in, T& record)
   {
      unsigned pos = 0;
      pos += unfold(in + pos, record.p_part_key);
      return pos;
   }
   static constexpr unsigned maxFoldLength() { return 0 + sizeof(Key::p_part_key); };
};

struct supplier_t {
   //  SF*2,000 are populated
   static constexpr int id = 2;
   struct Key {
      static constexpr int id = 2;
      /*
       * S_SUPPKEY numeric identifier
       */
      Integer s_supp_key;
   };
   /*
    * S_NAME fixed text, size 25: 'Supplier'||S_SUPPKEY
    * S_ADDRESS variable text, size 25 (city below)
    * S_CITY fixed text, size 10 (10/nation: S_NATION_PREFIX||(0-9)
    * S_NATION fixed text, size 15 (25 values, longest UNITED KINGDOM)
    * S_REGION fixed text, size 12 (5 values: longest MIDDLE EAST)
    * S_PHONE fixed text, size 15 (many values, format: 43-617-354-1222
    */
   Varchar<25> s_name;
   Varchar<25> s_address;
   Varchar<10> s_city;
   Varchar<15> s_nation;
   Varchar<12> s_region;
   Varchar<15> s_phone;

   template <class T>
   static unsigned foldKey(uint8_t* out, const T& record)
   {
      unsigned pos = 0;
      pos += fold(out + pos, record.s_supp_key);
      return pos;
   }
   template <class T>
   static unsigned unfoldKey(const uint8_t* in, T& record)
   {
      unsigned pos = 0;
      pos += unfold(in + pos, record.s_supp_key);
      return pos;
   }
   static constexpr unsigned maxFoldLength() { return 0 + sizeof(Key::s_supp_key); };
};

struct customer_t {
   //  SF*30,000 are populated
   static constexpr int id = 3;
   struct Key {
      static constexpr int id = 3;
      /*
       * C_CUSTKEY numeric identifier
       */
      Integer c_cust_key;
   };
   /*
    * C_NAME variable text, size 25 'Cutomer'||C_CUSTKEY
    * C_ADDRESS variable text, size 25 (city below)
    * C_CITY fixed text, size 10 (10/nation: C_NATION_PREFIX||(0-9)
    * C_NATION fixed text, size 15 (25 values, longest UNITED KINGDOM)
    * C_REGION fixed text, size 12 (5 values: longest MIDDLE EAST)
    * C_PHONE fixed text, size 15 (many values, format: 43-617-354-1222)
    * C_MKTSEGMENT fixed text, size 10 (longest is AUTOMOBILE)
    */
   Varchar<25> c_name;
   Varchar<25> c_address;
   Varchar<10> c_city;
   Varchar<15> c_nation;
   Varchar<12> c_region;
   Varchar<15> c_phone;
   Varchar<10> c_mkt_segment;

   template <class T>
   static unsigned foldKey(uint8_t* out, const T& record)
   {
      unsigned pos = 0;
      pos += fold(out + pos, record.c_cust_key);
      return pos;
   }
   template <class T>
   static unsigned unfoldKey(const uint8_t* in, T& record)
   {
      unsigned pos = 0;
      pos += unfold(in + pos, record.c_cust_key);
      return pos;
   }
   static constexpr unsigned maxFoldLength() { return 0 + sizeof(Key::c_cust_key); };
};

struct date_t {
   // 7 years of days
   // Anmerkung dev: start from 1992
   static constexpr int id = 4;
   struct Key {
      static constexpr int id = 4;
      /*
       * D_DATEKEY identifier, unique id -- e.g. 19980327 (what we use)
       */
      Integer d_date_key;
   };
   /*
    * D_DATE fixed text, size 18: e.g. December 22, 1998
    * D_DAYOFWEEK fixed text, size 8, Sunday..Saturday
    * D_MONTH fixed text, size 9: January, ..., December
    * D_YEAR unique value 1992-1998
    * D_YEARMONTHNUM numeric (YYYYMM)
    * D_YEARMONTH fixed text, size 7: (e.g.: Mar1998
    * D_DAYNUMINWEEK numeric 1-7
    * D_DAYNUMINMONTH numeric 1-31
    * D_DAYNUMINYEAR numeric 1-366
    * D_MONTHNUMINYEAR numeric 1-12
    * D_WEEKNUMINYEAR numeric 1-53
    * D_SELLINGSEASON text, size 12 (e.g.: Christmas)
    * D_LASTDAYINWEEKFL 1 bit
    * D_LASTDAYINMONTHFL 1 bit
    * D_HOLIDAYFL 1 bit
    * D_WEEKDAYFL 1 bit
    */
   Varchar<18> d_date;
   Varchar<9> d_day_of_week;  // Wednesday needs 9 and not 8
   Varchar<9> d_month;
   Integer d_year;
   Integer d_year_month_number;
   Varchar<7> d_year_month;
   Integer d_day_num_in_week;
   Integer d_day_num_in_month;
   Integer d_day_num_in_year;
   Integer d_month_num_in_year;
   Integer d_week_num_in_year;
   Varchar<12> d_selling_season;
   Integer d_last_day_in_week_fl;   // 1 bit
   Integer d_last_day_in_month_fl;  // 1 bit
   Integer d_holiday_fl;            // 1 bit
   Integer d_weekday_fl;            // 1 bit

   template <class T>
   static unsigned foldKey(uint8_t* out, const T& record)
   {
      unsigned pos = 0;
      pos += fold(out + pos, record.d_date_key);
      return pos;
   }
   template <class T>
   static unsigned unfoldKey(const uint8_t* in, T& record)
   {
      unsigned pos = 0;
      pos += unfold(in + pos, record.d_date_key);
      return pos;
   }
   static constexpr unsigned maxFoldLength() { return 0 + sizeof(Key::d_date_key); };
};
