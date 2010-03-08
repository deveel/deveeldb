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
using System.Collections;
using System.IO;
using System.Text;

using Deveel.Math;

namespace Deveel.Data {
	/// <summary>
	/// Provides static methods for transfering different types of 
	/// objects over a Data input/output stream.
	/// </summary>
	public class ObjectTransfer {
		///<summary>
		/// Makes an estimate of the size of the object.
		///</summary>
		///<param name="ob"></param>
		/// <remarks>
		/// This is useful for making a guess for how much this will take up.
		/// </remarks>
		///<returns></returns>
		///<exception cref="IOException"></exception>
		public static int SizeOf(Object ob) {
			if (ob == null)
				return 9;
			if (ob is StringObject)
				return (ob.ToString().Length*2) + 9;
			if (ob is BigNumber)
				return 15 + 9;
			/*
			if (ob is DateTime)
				return 8 + 9;
			if (ob is TimeSpan)
				return 8 + 9;
			*/
			if (ob is DateTime)
				return 28 + 9;
			if (ob is Interval)
				return 24 + 9;
			if (ob is bool)
				return 2 + 9;
			if (ob is ByteLongObject)
				return ((ByteLongObject) ob).Length + 9;
			if (ob is StreamableObject)
				return 5 + 9;
			//TODO: be more accurate...
			if (ob is UserObject)
				return 1000 + 9;

			throw new IOException("Unrecognised type: " + ob.GetType());
		}

		///<summary>
		/// Returns the exact size an object will take up when serialized.
		///</summary>
		///<param name="ob"></param>
		///<returns></returns>
		///<exception cref="IOException"></exception>
		public static int ExactSizeOf(Object ob) {
			if (ob == null)
				return 1;
			if (ob is StringObject)
				return (ob.ToString().Length * 2) + 1 + 4;
			if (ob is BigNumber) {
				BigNumber n = (BigNumber)ob;
				if (n.CanBeInt)
					return 4 + 1;
				if (n.CanBeLong)
					return 8 + 1;
				byte[] buf = n.ToByteArray();
				return buf.Length + 1 + 1 + 4 + 4;
			}
			/*
			if (ob is DateTime)
				return 8 + 1;
			if (ob is TimeSpan)
				return 8 + 1;
			*/
			if (ob is DateTime)
				return 6 + 2 + 1;
			if (ob is Interval)
				return 6 + 1;
			if (ob is bool)
				return 1 + 1;
			if (ob is ByteLongObject)
				return ((ByteLongObject)ob).Length + 1 + 8;
			if (ob is StreamableObject)
				return 1 + 1 + 4;
			if (ob is UserObject) {
				UserObject udt = (UserObject) ob;
				int size = 1 + 4;

				foreach(DictionaryEntry entry in udt) {
					size += ExactSizeOf(entry.Value);
				}

				return size;
			}
			
			throw new IOException("Unrecognised type: " + ob.GetType());
		}

		///<summary>
		/// Writes an object to the data output stream.
		///</summary>
		///<param name="output"></param>
		///<param name="ob"></param>
		///<exception cref="IOException"></exception>
		public static void WriteTo(BinaryWriter output, object ob) {
			if (ob == null) {
				output.Write((byte) 1);
			} else if (ob is StringObject) {
				byte[] buffer = Encoding.Unicode.GetBytes(ob.ToString());
				output.Write((byte) 18);
				output.Write(buffer.Length);
				output.Write(buffer, 0, buffer.Length);
			} else if (ob is BigNumber) {
				BigNumber n = (BigNumber) ob;
				if (n.CanBeInt) {
					output.Write((byte) 24);
					output.Write(n.ToInt32());
				} else if (n.CanBeLong) {
					output.Write((byte) 8);
					output.Write(n.ToInt64());
				} else {
					output.Write((byte) 7);
					output.Write((byte) n.State);
					output.Write(n.Scale);
					byte[] buf = n.ToByteArray();
					output.Write(buf.Length);
					output.Write(buf);
				}
			} 
			/*
			else if (ob is DateTime) {
				DateTime d = (DateTime) ob;
				output.Write((byte) 9);
				output.Write(d.Ticks);
			} else if (ob is TimeSpan) {
				TimeSpan t = (TimeSpan) ob;
				output.Write((byte) 10);
				output.Write(t.Ticks);
			} 
			*/
			else if (ob is DateTime) {
				DateTime d = (DateTime) ob;
				output.Write((byte)9);
				output.Write((byte)d.Year);
				output.Write((byte)d.Month);
				output.Write((byte)d.Day);
				output.Write((byte)d.Hour);
				output.Write((byte)d.Minute);
				output.Write((byte)d.Second);
				output.Write((short)d.Millisecond);
			} else if (ob is Interval) {
				Interval interval = (Interval) ob;
				output.Write((byte)10);
				output.Write((byte)interval.Years);
				output.Write((byte)interval.Months);
				output.Write((byte)interval.Days);
				output.Write((byte)interval.Hours);
				output.Write((byte)interval.Seconds);
				output.Write((byte)interval.Seconds);
			} else if (ob is Boolean) {
				bool b = (bool)ob;
				output.Write((byte)12);
				output.Write(b);
			} else if (ob is ByteLongObject) {
				ByteLongObject barr = (ByteLongObject)ob;
				output.Write((byte)15);
				byte[] arr = barr.ToArray();
				output.Write(arr.LongLength);
				output.Write(arr);
			} else if (ob is StreamableObject) {
				StreamableObject ob_head = (StreamableObject)ob;
				output.Write((byte)16);
				output.Write((byte)ob_head.Type);
				output.Write(ob_head.Size);
				output.Write(ob_head.Identifier);
			} else if (ob is UserObject) {
				UserObject ob_comp = (UserObject)ob;
				output.Write((byte)32);

				foreach (object value in ob_comp.Values) {
					WriteTo(output, value);
				}
			} else {
				throw new IOException("Unrecognised type: " + ob.GetType());
			}
		}

		/// <summary>
		/// Reads an object from the data input stream.
		/// </summary>
		/// <param name="input"></param>
		/// <returns></returns>
		public static object ReadFrom(BinaryReader input) {
			return ReadFrom(input, null);
		}

		/// <summary>
		/// Reads an object from the data input stream.
		/// </summary>
		/// <param name="input"></param>
		/// <param name="ttype"></param>
		/// <returns></returns>
		public static object ReadFrom(BinaryReader input, TType ttype) {
			byte type = input.ReadByte();

			switch (type) {
				case (1):
					return null;

				case (3):
					string str = input.ReadString();
					return StringObject.FromString(str);

				case (6): {
					int scale = input.ReadInt32();
					int blen = input.ReadInt32();
					byte[] buf = new byte[blen];
					input.Read(buf, 0, buf.Length);
					return BigNumber.Create(buf, scale, NumberState.None);
				}

				case (7): {
					NumberState state = (NumberState) input.ReadByte();
					int scale = input.ReadInt32();
					int blen = input.ReadInt32();
					byte[] buf = new byte[blen];
					input.Read(buf, 0, buf.Length);
					return BigNumber.Create(buf, scale, state);
				}

				case (8): {
					// 64-bit long numeric value
					long val = input.ReadInt64();
					return (BigNumber) val;
				}

				case (9): {
					byte year = input.ReadByte();
					byte month = input.ReadByte();
					byte day = input.ReadByte();
					byte hour = input.ReadByte();
					byte minute = input.ReadByte();
					byte second = input.ReadByte();
					short millis = input.ReadInt16();
					return new DateTime(year, month, day, hour, minute, second, millis);
				}
				case (10): {
					byte years = input.ReadByte();
					byte months = input.ReadByte();
					byte days = input.ReadByte();
					byte hours = input.ReadByte();
					byte minutes = input.ReadByte();
					byte seconds = input.ReadByte();
					return new Interval(years, months, days, hours, minutes, seconds);
				}
				case (12):
					return input.ReadBoolean();

				case (15): {
					long size = input.ReadInt64();
					byte[] arr = new byte[(int) size];
					input.Read(arr, 0, (int) size);
					return new ByteLongObject(arr);
				}

				case (16): {
					ReferenceType h_type = (ReferenceType) input.ReadByte();
					long h_size = input.ReadInt64();
					long h_id = input.ReadInt64();
					return new StreamableObject(h_type, h_size, h_id);
				}

				case (18): {
					// Handles strings > 64k
					int len = input.ReadInt32();
					byte[] buffer = new byte[len];
					input.Read(buffer, 0, len);
					return StringObject.FromString(Encoding.Unicode.GetString(buffer));
				}

				case (24): {
					// 32-bit int numeric value
					long val = input.ReadInt32();
					return (BigNumber) val;
				}
				case (32): {
					// a user-defined type structure is stored into the
					// Type in the TType
						if (ttype == null)
							throw new ArgumentNullException("ttype");

					UserType userType = ((TUserDefinedType) ttype).UserType;
					int fieldCount = userType.MemberCount;

					UserObject ob = new UserObject(userType);
					for (int i = 0; i < fieldCount; i++) {
						UserTypeAttribute attribute = userType.GetAttribute(i);

						string fieldName = attribute.Name;
						TType fieldType = attribute.Type;
						object fieldValue = ReadFrom(input, fieldType);

						ob.SetValue(fieldName, fieldValue);
					}

					return ob;
				}

				default:
					throw new IOException("Unrecognised type: " + type);

			}
		}
	}
}