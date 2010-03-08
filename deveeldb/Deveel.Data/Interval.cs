// 
//  Copyright 2010  Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.Text;

namespace Deveel.Data {
	[Serializable]
	public struct Interval : IComparable {
		public Interval(int years, int months, int days, int hours, int minutes, int seconds) {
			weeks = days / 7;
			this.days = days % 7;
			this.hours = hours;
			this.minutes = minutes;
			this.seconds = seconds;
			this.years = years;
			this.months = months;
		}

		public Interval(int days, int hours, int minutes, int seconds)
			: this(0, 0, days, hours, minutes, seconds) {
		}

		public Interval(int years, int months)
			: this(years, months, 0, 0, 0, 0) {
		}

		public Interval(DateTime start, DateTime end) {
			// if the start date is over the end, swap it
			if (start > end) {
				DateTime dtTemp = start;
				start = end;
				end = dtTemp;
			}

			// calculate the years
			years = end.Year - start.Year;
			if (years > 0) {
				if (end.Month < start.Month) {
					years = years - 1;
				} else if (end.Month == start.Month) {
					if (end.Day < start.Day) {
						years = years - 1;
					} else if (end.Day == start.Day) {
						if (end.Hour < start.Hour) {
							years = years - 1;
						} else if (end.Hour == start.Hour) {
							if (end.Minute < start.Minute) {
								years = years - 1;
							} else if (end.Minute == start.Minute) {
								if (end.Second < start.Second) {
									years = years - 1;
								}
							}
						}
					}
				}
			}

			// now calculate the months
			months = end.Month - start.Month;
			if (end.Month < start.Month) {
				months = 12 - start.Month + end.Month;
			}
			if (months > 0) {
				if (end.Day < start.Day) {
					months--;
				} else if (end.Day == start.Day) {
					if (end.Hour < start.Hour) {
						months--;
					} else if (end.Hour == start.Hour) {
						if (end.Minute < start.Minute) {
							months--;
						} else if (end.Minute == start.Minute) {
							if (end.Second < start.Second) {
								months--;
							}
						}
					}
				}
			}

			// days
			days = end.Day - start.Day;
			if (end.Day < start.Day) {
				days = DateTime.DaysInMonth(start.Year, start.Month) - start.Day + end.Day;
			}
			if (days > 0) {
				if (end.Hour < start.Hour) {
					days--;
				} else if (end.Hour == start.Hour) {
					if (end.Minute < start.Minute) {
						days--;
					} else if (end.Minute == start.Minute) {
						if (end.Second < start.Second) {
							days--;
						}
					}
				}
			}

			// weeks
			weeks = days / 7;

			// adjust the days
			days = days % 7;

			// hours
			hours = end.Hour - start.Hour;
			if (end.Hour < start.Hour) {
				hours = 24 - start.Hour + end.Hour;
			}
			if (hours > 0) {
				if (end.Minute < start.Minute) {
					hours--;
				} else if (end.Minute == start.Minute) {
					if (end.Second < start.Second) {
						hours--;
					}
				}
			}

			minutes = end.Minute - start.Minute;
			if (end.Minute < start.Minute) {
				minutes = 60 - start.Minute + end.Minute;
			}
			if (minutes > 0) {
				if (end.Second < start.Second) {
					minutes--;
				}
			}

			seconds = end.Second - start.Second;
			if (end.Second < start.Second) {
				seconds = 60 - start.Second + end.Second;
			}
		}

		private int years;
		private int months;
		private int days;
		private readonly int weeks;
		private int hours;
		private int minutes;
		private int seconds;
		
		public static readonly Interval Zero = new Interval(0, 0, 0, 0, 0, 0);

		public int Years {
			get { return years; }
		}

		public int Months {
			get { return months; }
		}

		public int Weeks {
			get { return weeks; }
		}

		public int Days {
			get { return days; }
		}

		public int Hours {
			get { return hours; }
		}

		public int Minutes {
			get { return minutes; }
		}

		public int Seconds {
			get { return seconds; }
		}

		public bool IsYearToMonth {
			get {
				return (years > 0 || months > 0) &&
				       (days == 0 && hours == 0 && 
					   minutes == 0 && seconds == 0);
			}
		}

		public override bool Equals(object obj) {
			if (!(obj is Interval))
				return false;

			Interval other = (Interval) obj;

			// a fast comparison
			if (IsYearToMonth && !other.IsYearToMonth)
				return false;

			return CompareTo(other) == 0;
		}

		public override int GetHashCode() {
			int code = 0;
			for (int i = 0; i < 6; i++) {
				int c = GetField(i);
				code ^= c;
			}

			return code;
		}

		public TimeSpan ToTimeSpan() {
			if (IsYearToMonth)
				throw new InvalidOperationException("Cannot convert a YEAR TO MONTH interval to a TimeStamp.");

			return new TimeSpan(days, hours, minutes, seconds);
		}

		private static void ParseYearToMonth(string s, out int years, out int months) {
			int index = s.IndexOf('-');
			string yearComp = s;
			string monthComp = "0";
			if (index != -1) {
				monthComp = yearComp.Substring(index + 1);
				yearComp = yearComp.Substring(0, index);
			}

			years = Int32.Parse(yearComp);
			months = Int32.Parse(monthComp);
		}

		private static void ParseDayToSecond(string s, out int days, out int hours, out int minutes, out int seconds) {
			int index = s.IndexOf(' ');
			string daysComp = "0";
			string hoursComp = s;
			string minutesComp = "0";
			string secondsComp = "0";

			if (index != -1) {
				daysComp = hoursComp.Substring(0, index);
				hoursComp = hoursComp.Substring(index + 1);

				index = hoursComp.IndexOf(':');
				if (index != -1) {
					minutesComp = hoursComp.Substring(index + 1);
					hoursComp = hoursComp.Substring(0, index);

					index = minutesComp.IndexOf(':');
					if (index != -1) {
						secondsComp = minutesComp.Substring(index + 1);
						minutesComp = minutesComp.Substring(0, index);
					}
				}
			}

			days = Int32.Parse(daysComp);
			hours = Int32.Parse(hoursComp);
			minutes = Int32.Parse(minutesComp);
			seconds = Int32.Parse(secondsComp);
		}

		public static Interval Parse(string s, IntervalForm type) {
			if (type == IntervalForm.YearToMonth) {
				int years, months;
				ParseYearToMonth(s, out years, out months);
				return new Interval(years, months);
			}
			if (type == IntervalForm.DayToSecond) {
				int days, hours, minutes, seconds;
				ParseDayToSecond(s, out days, out hours, out minutes, out seconds);
				return new Interval(days, hours, minutes, seconds);
			}

			int index = s.IndexOf(' ');
			if (index == -1)
				return Parse(s, IntervalForm.YearToMonth);

			string s1 = s.Substring(0, index);
			string s2 = s.Substring(index + 1);

			Interval yearToMonth = Parse(s1, IntervalForm.YearToMonth);
			Interval dayToSecond = Parse(s2, IntervalForm.DayToSecond);

			return new Interval(yearToMonth.Years, yearToMonth.Months, dayToSecond.Days, dayToSecond.Hours,
			                    dayToSecond.Minutes, dayToSecond.Seconds);
		}

		public override string ToString() {
			if (IsYearToMonth)
				return ToString(IntervalForm.YearToMonth);
			return ToString(IntervalForm.Full);
		}

		private void YearToMonthString(StringBuilder sb) {
			sb.Append(years);
			if (months != 0)
				sb.Append("-").Append(months);			
		}

		private void DayToSecondString(StringBuilder sb) {
			if (days > 0)
				sb.Append(days).Append(" ");

			sb.Append(hours);
			sb.Append(":");
			sb.Append(minutes);
			sb.Append(":");
			sb.Append(seconds);
		}

		public string ToString(IntervalForm type) {
			StringBuilder sb = new StringBuilder();

			if (type == IntervalForm.YearToMonth) {
				YearToMonthString(sb);
			} else if (type == IntervalForm.DayToSecond) {
				DayToSecondString(sb);
			} else {
				YearToMonthString(sb);
				sb.Append(" ");
				DayToSecondString(sb);
			}

			return sb.ToString();
		}

		public int GetField(IntervalField field) {
			return GetField((int) field);
		}

		#region Implementation of IComparable

		private int GetField(int index) {
			switch(index) {
				case 0: return years;
				case 1: return months;
				case 2: return days;
				case 3: return hours;
				case 4: return minutes;
				case 5: return seconds;
				default:
					throw new ArgumentOutOfRangeException();
			}
		}

		public int CompareTo(object obj) {
			if (!(obj is Interval))
				throw new ArgumentException();

			Interval other = (Interval) obj;

			for (int i = 0; i < 6; i++) {
				int i1 = GetField(i),
				    i2 = other.GetField(i);
				int c = i1.CompareTo(i2);
				if (c != 0)
					return c;
			}

			return 0;
		}

		#endregion

		public Interval Add(Interval value) {
			years += value.years;
			months += value.months;
			days += value.days;
			hours += value.hours;
			minutes += value.minutes;
			seconds += value.seconds;
			return this;
		}
	}
}