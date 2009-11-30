//  
//  ObjectTransfer.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.IO;
using System.Text;

using Deveel.Math;

namespace Deveel.Data {
	/// <summary>
	/// Provides static methods for transfering different types of 
	/// objects over a Data input/output stream.
	/// </summary>
	public
#if NET_2_0
	static
#else
	sealed
#endif
	class ObjectTransfer {
#if !NET_2_0
		private ObjectTransfer() {
		}
#endif

		///<summary>
		/// Makes an estimate of the size of the object.
		///</summary>
		///<param name="ob"></param>
		/// <remarks>
		/// This is useful for making a guess for how much this will take up.
		/// </remarks>
		///<returns></returns>
		///<exception cref="IOException"></exception>
		public static int SizeOf(object ob) {
			if (ob == null)
				return 9;
			if (ob is StringObject)
				return (ob.ToString().Length * 2) + 9;
			if (ob is BigNumber)
				return 15 + 9;
			if (ob is DateTime)
				return 8 + 9;
			if (ob is TimeSpan)
				return 8 + 9;
			if (ob is bool)
				return 2 + 9;
			if (ob is ByteLongObject)
				return ((ByteLongObject)ob).Length + 9;
			if (ob is StreamableObject)
				return 5 + 9;
			if (ob is IUserDefinedType)
				//TODO: be more specific...
				return 1000 + 9;
			
			throw new IOException("Unrecognised type: " + ob.GetType());
		}

		///<summary>
		/// Returns the exact size an object will take up when serialized.
		///</summary>
		///<param name="ob">The object for which to extimate the exact
		/// size of.</param>
		///<returns></returns>
		///<exception cref="IOException"></exception>
		public static int ExactSizeOf(Object ob) {
			if (ob == null)
				return 1;
			if (ob is StringObject)
				return (ob.ToString().Length*2) + 1 + 4;
			if (ob is BigNumber) {
				BigNumber n = (BigNumber) ob;
				if (n.CanBeInt)
					return 4 + 1;
				if (n.CanBeLong)
					return 8 + 1;
				byte[] buf = n.ToByteArray();
				return buf.Length + 1 + 1 + 4 + 4;
			}
			if (ob is DateTime)
				return 8 + 1;
			if (ob is TimeSpan)
				return 8 + 1;
			if (ob is bool)
				return 1 + 1;
			if (ob is ByteLongObject)
				return ((ByteLongObject) ob).Length + 1 + 8;
			if (ob is StreamableObject)
				return 1 + 1 + 4;
			if (ob is IUserDefinedType) {
				int size = 1;
				IUserDefinedType udt = (IUserDefinedType) ob;
				UserTypeDef tableDef = udt.TypeDef;

				string typeName = tableDef.Name.ToString();
				size += 4 + typeName.Length*2;

				int colCount = tableDef.MemberCount;
				for (int i = 0; i < colCount; i++) {
					object value = udt.GetValue(i);
					size += 1 + ExactSizeOf(value);
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
		public static void WriteTo(BinaryWriter output, Object ob) {
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
			} else if (ob is DateTime) {
				DateTime d = (DateTime) ob;
				output.Write((byte) 9);
				output.Write(d.Ticks);
			} else if (ob is TimeSpan) {
				TimeSpan t = (TimeSpan) ob;
				output.Write((byte) 10);
				output.Write(t.Ticks);
			} else if (ob is Boolean) {
				Boolean b = (Boolean) ob;
				output.Write((byte) 12);
				output.Write(b);
			} else if (ob is ByteLongObject) {
				ByteLongObject barr = (ByteLongObject) ob;
				output.Write((byte) 15);
				byte[] arr = barr.ToArray();
				output.Write(arr.LongLength);
				output.Write(arr);
			} else if (ob is StreamableObject) {
				StreamableObject ob_head = (StreamableObject) ob;
				output.Write((byte) 16);
				output.Write((byte) ob_head.Type);
				output.Write(ob_head.Size);
				output.Write(ob_head.Identifier);
			} else if (ob is IUserDefinedType) {
				IUserDefinedType udt = (IUserDefinedType) ob;
				output.Write((byte) 34);

				UserTypeDef tableDef = udt.TypeDef;
				string typeName = tableDef.Name.ToString();
				byte[] buffer = Encoding.Unicode.GetBytes(typeName);
				output.Write(buffer.Length);
				output.Write(buffer);

				int colCount = tableDef.MemberCount;
				for (int i = 0; i < colCount; i++) {
					object colValue = udt.GetValue(i);
					WriteTo(output, colValue);
				}
			} else {
				throw new IOException("Unrecognised type: " + ob.GetType());
			}
		}

		public static Object ReadFrom(BinaryReader input) {
			return ReadFrom(input, null);
		}

		/// <summary>
		/// Reads an object from the data input stream.
		/// </summary>
		/// <param name="input"></param>
		/// <returns></returns>
		public static Object ReadFrom(BinaryReader input, IUDTHandler udtHandler) {
			byte type = input.ReadByte();

			switch (type) {
				case (1):
					return null;

				case (3):
					String str = input.ReadString();
					return StringObject.FromString(str);

				case (6): {
						int scale = input.ReadInt32();
						int blen = input.ReadInt32();
						byte[] buf = new byte[blen];
						input.Read(buf, 0, buf.Length);
						return BigNumber.Create(buf, scale, NumberState.None);
					}

				case (7): {
						NumberState state = (NumberState)input.ReadByte();
						int scale = input.ReadInt32();
						int blen = input.ReadInt32();
						byte[] buf = new byte[blen];
						input.Read(buf, 0, buf.Length);
						return BigNumber.Create(buf, scale, state);
					}

				case (8): {
						// 64-bit long numeric value
						long val = input.ReadInt64();
						return (BigNumber)val;
					}

				case (9):
					long time = input.ReadInt64();
					return new DateTime(time);
				case (10):
					long ticks = input.ReadInt64();
					return new TimeSpan(ticks);

				case (12):
					return input.ReadBoolean();

				case (15): {
						long size = input.ReadInt64();
						byte[] arr = new byte[(int)size];
						input.Read(arr, 0, (int)size);
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
						long val = (long)input.ReadInt32();
						return (BigNumber)val;
					}
				case (34): {
					if (udtHandler == null)
						throw new NotSupportedException("Reading UDTs outside a context is not supported.");

					int nameLen = input.ReadInt32();
					byte[] nameBuffer = new byte[nameLen];
					input.Read(nameBuffer, 0, nameLen);
					TableName name = new TableName(Encoding.Unicode.GetString(nameBuffer));

					IUserDefinedType udt = udtHandler.GetUserDefinedType(name);
					if (udt == null)
						throw new InvalidOperationException("The user-defined type (UDT) " + name + " was not found in the context.");

					int colCount = udt.TypeDef.MemberCount;
					for (int i = 0; i < colCount; i++) {
						object value = ReadFrom(input, udtHandler);
						udt.SetValue(i, value);
					}

					return udt;
				}

				default:
					throw new IOException("Unrecognised type: " + type);

			}
		}
	}
}