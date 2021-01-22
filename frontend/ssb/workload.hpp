/**
 * @file workload.hpp
 * @brief Functions to execute SSB transactions.
 *
 */

#include <cstdio>
#include <cstring>
#include <ctime>
#include <iostream>

DEFINE_bool(order_wdc_index, true, "");
atomic<u64> scanned_elements = 0;

// load

Integer scale_factor;
// -------------------------------------------------------------------------------------
static constexpr INTEGER OL_I_ID_C = 7911;  // C for getItemID: in range [0, 8191]
static constexpr INTEGER C_ID_C = 259;      // C for getCustomerID: in range [0, 1023]
// NOTE: TPC-C 2.1.6.1 specifies that abs(C_LAST_LOAD_C - C_LAST_RUN_C) must
// be within [65, 119]
static constexpr INTEGER C_LAST_LOAD_C = 157;  // in range [0, 255]
static constexpr INTEGER C_LAST_RUN_C = 223;   // in range [0, 255]
// -------------------------------------------------------------------------------------
static constexpr INTEGER ITEMS_NO = 100000;  // 100K

// -------------------------------------------------------------------------------------
// RandomGenerator functions
// -------------------------------------------------------------------------------------

// [0, n)
Integer rnd(Integer n)
{
   return leanstore::utils::RandomGenerator::getRand(0, n);
}

// [low, high]
Integer uRand(Integer low, Integer high)
{
   return rnd(high - low + 1) + low;
}

// [fromId, toId]
Integer randomId(Integer fromId, Integer toId)
{
   return uRand(fromId, toId);
}

Integer uRandExcept(Integer low, Integer high, Integer v)
{
   if (high <= low)
      return low;
   Integer r = uRand(low, high - 1);
   if (r >= v)
      return r + 1;
   else
      return r;
}

Integer nURand(Integer A, Integer x, Integer y, Integer C = 42)
{
   // TPC-C random is [a,b] inclusive
   // in standard: NURand(A, x, y) = (((random(0, A) | random(x, y)) + C) % (y - x + 1)) + x
   return (((uRand(0, A) | uRand(x, y)) + C) % (y - x + 1)) + x;
}

// 0-9A-Za-z
template <int maxLength>
Varchar<maxLength> randomASCIIString(Integer minLenStr, Integer maxLenStr)
{
   assert(maxLenStr <= maxLength);
   Integer len = uRand(minLenStr, maxLenStr);
   Varchar<maxLength> result;
   for (Integer index = 0; index < len; index++) {
      Integer i = rnd(62);
      if (i < 10)  // 0-9
         result.append(48 + i);
      else if (i < 36)  // A-Z
         result.append(64 - 10 + i);
      else  // a-z
         result.append(96 - 36 + i);
   }
   return result;
}

template <int maxLength>
Varchar<maxLength> randomASCIIStringMaybeOriginal(Integer minLenStr, Integer maxLenStr)
{
   Varchar<maxLength> data = randomASCIIString<maxLength>(minLenStr, maxLenStr);
   if (rnd(10) == 0) {
      data.length = rnd(data.length - 8);
      data = data || Varchar<10>("ORIGINAL");
   }
   return data;
}
// 0-9
Varchar<16> randomNumString(Integer minLenStr, Integer maxLenStr)
{
   assert(maxLenStr <= 16);
   Integer len = uRand(minLenStr, maxLenStr);
   Varchar<16> result;
   for (Integer i = 0; i < len; i++)
      result.append(48 + rnd(10));  // 0-9
   return result;
}

/**
 * @brief Returns namePart from list of Strings.
 *
 * @param id position in list of Strings to take
 * @return Varchar<16> VarChar representation of String
 */
Varchar<16> namePart(Integer id)
{
   assert(id < 10);
   Varchar<16> data[] = {"Bar", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};
   return data[id];
}

/**
 * @brief Generate name from partStrings.
 *
 * @param id id for which to generate name
 * @return Varchar<16> Varchar representation of the name
 */
Varchar<16> genName(Integer id)
{
   return namePart((id / 100) % 10) || namePart((id / 10) % 10) || namePart(id % 10);
}

Numeric randomNumeric(Numeric min, Numeric max)
{
   double range = (max - min);
   double div = RAND_MAX / range;
   return min + (leanstore::utils::RandomGenerator::getRandU64() / div);
}

Varchar<9> randomZip()
{
   Integer id = rnd(10000);
   Varchar<9> result;
   result.append(48 + (id / 1000));
   result.append(48 + (id / 100) % 10);
   result.append(48 + (id / 10) % 10);
   result.append(48 + (id % 10));
   return result || Varchar<9>("11111");
}

inline Integer getItemID()
{
   // OL_I_ID_C
   return nURand(8191, 1, ITEMS_NO, OL_I_ID_C);
}
inline Integer getCustomerID()
{
   // C_ID_C
   return nURand(1023, 1, 3000, C_ID_C);
}
inline Integer getNonUniformRandomLastNameForRun()
{
   // C_LAST_RUN_C
   return nURand(255, 0, 999, C_LAST_RUN_C);
}
inline Integer getNonUniformRandomLastNameForLoad()
{
   // C_LAST_LOAD_C
   return nURand(255, 0, 999, C_LAST_LOAD_C);
}

// Date Functions
template <int maxLength>
Varchar<maxLength> getDateFormat(tm* day, const char* format)
{
   char value[maxLength + 1] = {0};
   int length = strftime(value, maxLength + 1, format, day);
   if (length <= 0 && maxLength) {
      perror("Error in GetDateFormat");
      fprintf(stderr, "%d, %s, %ld\n", maxLength, format, std::strlen(format));
   }
   Varchar<maxLength> returner(value);
   returner.length = length;
   return returner;
}

Varchar<18> getDateString(tm* day)
{
   return getDateFormat<18>(day, "%B %d, %Y");
}
Varchar<9> getDayOfWeekString(tm* day)
{
   return getDateFormat<9>(day, "%A");
}
Varchar<9> getMonthString(tm* day)
{
   return getDateFormat<9>(day, "%B");
}
Varchar<7> getYearMonthString(tm* day)
{
   return getDateFormat<7>(day, "%b%Y");
}
// TODO: Unimplemented
Varchar<12> getSellingSeasonString(tm* day)
{
   return "SUMMER";
}
Integer getYear(tm* day)
{
   return 1990 + day->tm_year;
}
Integer getDayNumInWeek(tm* day)
{
   Integer weekday = day->tm_wday;
   if (weekday == 0)
      return 7;
   return weekday;
}
Integer getDayNumInMonth(tm* day)
{
   return day->tm_mday;
}
Integer getDayNumInYear(tm* day)
{
   return day->tm_yday + 1;
}
Integer getMonthNumInYear(tm* day)
{
   return 1 + day->tm_mon;
}
Integer getYearMonthNumber(tm* day)
{
   return getYear(day) * 100 + getMonthNumInYear(day);
}
// TODO: Unimplemented
Integer getWeekNumInYear(tm* day)
{
   return 0;
}
Integer getLastDayInWeekFL(tm* day)
{
   if (getDayNumInWeek(day) == 7)
      return 1;
   return 0;
}
// TODO: Unimplemented
Integer getLastDayInMonthFL(tm* day)
{
   return 0;
}
// TODO: Unimplemented
Integer getHolidayFL(tm* day)
{
   return 0;
}
Integer getWeekdayFL(tm* day)
{
   if (getDayNumInWeek(day) <= 6)
      return 1;
   return 0;
}

// -------------------------------------------------------------------------------------
// Functions to add data to dataset e.g. initialize dataset
// -------------------------------------------------------------------------------------

void loadDate()
{
   // Starting from 1.1.1992
   tm start_time;
   tm* thisday = &start_time;
   strptime("01 01 1992 00:00:00", "%d %m %Y %H:%M:%S", thisday);
   mktime(thisday);
   tm end_time;
   strptime("01 01 1999 00:00:00", "%d %m %Y %H:%M:%S", &end_time);
   mktime(&end_time);

   for (Integer i = 1; difftime(mktime(&start_time), mktime(&end_time)); i++) {
      date_table.insert({i}, {
                                 getDateString(thisday),
                                 getDayOfWeekString(thisday),
                                 getMonthString(thisday),
                                 getYear(thisday),
                                 getYearMonthNumber(thisday),
                                 getYearMonthString(thisday),
                                 getDayNumInWeek(thisday),
                                 getDayNumInMonth(thisday),
                                 getDayNumInYear(thisday),
                                 getMonthNumInYear(thisday),
                                 getWeekNumInYear(thisday),
                                 getSellingSeasonString(thisday),
                                 getLastDayInWeekFL(thisday),
                                 getLastDayInMonthFL(thisday),
                                 getHolidayFL(thisday),
                                 getWeekdayFL(thisday),
                             });
      thisday->tm_mday += 1;
      mktime(thisday);
      // thisday = gmtime(&tomorrow);
   }
   date_table.scan(
       {},
       [&](const date_t::Key, const date_t entry) {
          Varchar<18> tmpEntry(entry.d_date);
          cout << tmpEntry.toString() << endl;
          return true;
       },
       [&]() {});
}
// -------------------------------------------------------------------------------------
// Functions to execute operations on data
// -------------------------------------------------------------------------------------
