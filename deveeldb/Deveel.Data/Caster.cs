//  
//  Caster.cs
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
using System.Reflection;
using System.Text;

using Deveel.Math;

namespace Deveel.Data {
	/// <summary>
	/// Methods to choose and perform casts from database type to native types.
	/// </summary>
	internal class Caster {

		///<summary>
		/// The cost to cast to the closest .NET primitive type.
		///</summary>
		public const int PrimitiveCost = 100;

		///<summary>
		/// The cost to cast to the closes .NET object type.
		///</summary>
		public const int ObjectCost = 200;

		/// <summary>
		/// The maximum positive byte value as a BigNumber.
		/// </summary>
		private readonly static BigNumber maxBigNumByte = Byte.MaxValue;

		/// <summary>
		/// The maximum positive byte value as a BigNumber.
		/// </summary>
		private readonly static BigNumber minBigNumByte = Byte.MinValue;

		/// <summary>
		/// The maximum positive short value as a BigNumber.
		/// </summary>
		private readonly static BigNumber maxBigNumShort = Int16.MaxValue;

		/// <summary>
		/// The maximum positive short value as a BigNumber.
		/// </summary>
		private readonly static BigNumber minBigNumShort = Int16.MinValue;

		/// <summary>
		/// The maximum positive integer value as a BigNumber.
		/// </summary>
		private readonly static BigNumber maxBigNumInt = Int32.MaxValue;

		/// <summary>
		/// The maximum positive integer value as a BigNumber.
		/// </summary>
		private readonly static BigNumber minBigNumInt = Int32.MinValue;

		/// <summary>
		/// The maximum positive long value as a BigNumber.
		/// </summary>
		private readonly static BigNumber maxBigNumLong = Int64.MaxValue;

		/// <summary>
		/// The maximum positive long value as a BigNumber.
		/// </summary>
		private readonly static BigNumber minBigNumLong = Int64.MinValue;

		/// <summary>
		/// The maximum positive float value as a BigNumber.
		/// </summary>
		private readonly static BigNumber maxBigNumFloat = Single.MaxValue;

		/// <summary>
		/// The minimum positive float value as a BigNumber.
		/// </summary>
		private static readonly BigNumber minBigNumFloat = Single.MinValue;

		/// <summary>
		/// The maximum positive double value as a BigNumber.
		/// </summary>
		private static readonly BigNumber maxBigNumDouble = Double.MaxValue;

		///<summary>
		/// Find any OBJECTs in the args and deserialize them into real objects.
		///</summary>
		///<param name="args">The args to deserialize.  Any OBJECT args are
		/// converted in-place to a new TObject with a value which is
		/// the deserialized object.</param>
		public static void DeserializeObjects(TObject[] args) {
			for (int i = 0; i < args.Length; i++) {
				SqlType sqlType = args[i].TType.SQLType;
				if (sqlType != SqlType.Object) {
					continue;	// not a OBJECT
				}
				Object argVal = args[i].Object;
				if (!(argVal is ByteLongObject)) {
					continue;	// not ByteLongObject, we don't know how to deserialize
				}
				Object obj = ObjectTranslator.Deserialize((ByteLongObject)argVal);
				args[i] = new TObject(args[i].TType, obj);
			}
		}

		///<summary>
		/// Search for the best constructor that we can use with the given
		/// argument types.
		///</summary>
		///<param name="constructs">The set of constructors from which to select.</param>
		///<param name="args">The SQL types of the database arguments to be passed
		/// to the constructor.</param>
		///<returns>
		/// Returns the constructor with the lowest cost, or null if there are no 
		/// constructors that match the args.
		/// </returns>
		public static ConstructorInfo FindBestConstructor(ConstructorInfo[] constructs, TObject[] args) {
			int bestCost = 0;		// not used if bestConstructor is null
			ConstructorInfo bestConstructor = null;
			SqlType[] argSqlTypes = GetSqlTypes(args);
			for (int i = 0; i < constructs.Length; ++i) {
				ParameterInfo[] targets = constructs[i].GetParameters();
				Type[] target_types = new Type[targets.Length];
				for (int j = 0; j < target_types.Length; j++) {
					target_types[j] = targets[j].ParameterType;
				}
				int cost = GetCastingCost(args, argSqlTypes, target_types);
				if (cost < 0) {
					continue;		// not a usable constructor
				}
				if (bestConstructor == null || cost < bestCost) {
					bestCost = cost;	// found a better one, remember it
					bestConstructor = constructs[i];
				}
			}
			return bestConstructor;	// null if we didn't find any
		}

		///<summary>
		/// Get the SQL types for the given database arguments.
		///</summary>
		///<param name="args">The database args.</param>
		///<returns>
		/// Returns the SQL types of the args.
		/// </returns>
		public static SqlType[] GetSqlTypes(TObject[] args) {
			SqlType[] sqlTypes = new SqlType[args.Length];
			for (int i = 0; i < args.Length; i++) {
				sqlTypes[i] = GetSqlType(args[i]);
			}
			return sqlTypes;
		}

		///<summary>
		/// Get the SQL type for a database argument.
		///</summary>
		///<param name="arg">The database argument.</param>
		/// <remarks>
		/// If the actual value does not fit into the declared type, the returned
		/// type is widened as required for the value to fit.
		/// </remarks>
		///<returns>
		/// Returns the SQL type of the arg.
		/// </returns>
		public static SqlType GetSqlType(TObject arg) {
			SqlType sqlType = arg.TType.SQLType;
			Object argVal = arg.Object;
			if (!(argVal is BigNumber)) {
				return sqlType;	// We have special checks only for numeric values
			}
			BigNumber b = (BigNumber)argVal;
			BigNumber bAbs;
			switch (sqlType) {
				case SqlType.Numeric:
				case SqlType.Decimal:
					// If the type is NUMERIC or DECIMAL, then look at the data value
					// to see if it can be narrowed to int, long or double.
					if (b.CanBeInt) {
						sqlType = SqlType.Integer;
					} else if (b.CanBeLong) {
						sqlType = SqlType.BigInt;
					} else {
						bAbs = b.Abs();
						if (b.Scale == 0) {
							if (bAbs.CompareTo(maxBigNumInt) <= 0) {
								sqlType = SqlType.Integer;
							} else if (bAbs.CompareTo(maxBigNumLong) <= 0) {
								sqlType = SqlType.BigInt;
							}
						} else if (bAbs.CompareTo(maxBigNumDouble) <= 0) {
							sqlType = SqlType.Double;
						}
					}
					// If we can't translate NUMERIC or DECIMAL to int, long or double,
					// then leave it as is.
					break;
				case SqlType.Bit:
					if (b.CanBeInt) {
						int n = b.ToInt32();
						if (n == 0 || n == 1) {
							return sqlType;	// Allowable Bit value
						}
					}
					// The value does not fit in a BIT, move up to a TINYINT
					sqlType = SqlType.TinyInt;
					goto case SqlType.TinyInt;
				// FALL THROUGH
				case SqlType.TinyInt:
					if (b.CompareTo(maxBigNumByte) <= 0 &&
					b.CompareTo(minBigNumByte) >= 0) {
						return sqlType;	// Fits in a TINYINT
					}
					// The value does not fit in a TINYINT, move up to a SMALLINT
					sqlType = SqlType.SmallInt;
					goto case SqlType.SmallInt;
				// FALL THROUGH
				case SqlType.SmallInt:
					if (b.CompareTo(maxBigNumShort) <= 0 &&
					b.CompareTo(minBigNumShort) >= 0) {
						return sqlType;	// Fits in a SMALLINT
					}
					// The value does not fit in a SMALLINT, move up to a INTEGER
					sqlType = SqlType.Integer;
					goto case SqlType.Integer;
				// FALL THROUGH
				case SqlType.Integer:
					if (b.CompareTo(maxBigNumInt) <= 0 &&
					b.CompareTo(minBigNumInt) >= 0) {
						return sqlType;	// Fits in a INTEGER
					}
					// The value does not fit in a INTEGER, move up to a BIGINT
					sqlType = SqlType.BigInt;
					// That's as far as we go
					break;
				case SqlType.Real:
					bAbs = b.Abs();
					if (bAbs.CompareTo(maxBigNumFloat) <= 0 &&
					(bAbs.CompareTo(minBigNumFloat) >= 0 ||
					 b.ToDouble() == 0.0)) {
						return sqlType;	// Fits in a REAL
					}
					// The value does not fit in a REAL, move up to a DOUBLE
					sqlType = SqlType.Double;
					break;
				default:
					break;
			}
			return sqlType;
		}

		///<summary>
		/// Get a string giving the database types of all of the arguments.
		///</summary>
		///<param name="args">The arguments.</param>
		/// <remarks>
		/// Useful for error messages.
		/// </remarks>
		///<returns>
		/// Returns a string with the types of all of the arguments, using 
		/// comma as a separator.
		/// </returns>
		public static String GetArgTypesString(TObject[] args) {
			StringBuilder sb = new StringBuilder();
			for (int n = 0; n < args.Length; n++) {
				if (n > 0) {
					sb.Append(",");
				}
				if (args[n] == null) {
					sb.Append("null");
				} else {
					SqlType sqlType = GetSqlType(args[n]);
					String typeName;
					if (sqlType == SqlType.Object) {
						Object argObj = args[n].Object;
						if (argObj == null) {
							typeName = "null";
						} else {
							typeName = argObj.GetType().FullName;
						}
					} else {
						typeName = sqlType.ToString().ToUpper();
					}
					sb.Append(typeName);
				}
			}
			return sb.ToString();
		}


		/// <summary>
		/// Get the cost for casting the given arg types to the 
		/// desired target types.
		/// </summary>
		/// <param name="args">The database arguments from which we are casting.</param>
		/// <param name="argSqlTypes">The SQL types of the args.</param>
		/// <param name="targets">The types to which we are casting.</param>
		/// <returns>
		/// Returns the cost of doing the cast for all arguments, or -1 
		/// if the args can not be cast to the targets.
		/// </returns>
		static int GetCastingCost(TObject[] args, SqlType[] argSqlTypes, Type[] targets) {
			if (targets.Length != argSqlTypes.Length) {
				return -1;		// wrong number of args
			}

			// Sum up the cost of converting each arg
			int totalCost = 0;
			for (int n = 0; n < argSqlTypes.Length; ++n) {
				int argCost = GetCastingCost(args[n], argSqlTypes[n], targets[n]);
				if (argCost < 0) {
					return -1;		//can't cast this arg type
				}
				int positionalCost = argCost * n / 10000;
				//Add a little bit to disambiguate constructors based on
				//argument position.  This gives preference to earlier
				//argument in cases where the cost of two sets of
				//targets for the same set of args would otherwise
				//be the same.
				totalCost += argCost + positionalCost;
			}
			return totalCost;
		}

		// These arrays are used in the GetCastingCost method below.
		private static readonly String[] bitPrims = { "bool" };
		private static readonly Type[] bitTypes = { typeof(bool) };

		private static readonly String[] tinyPrims = { "byte", "short", "int", "long" };
		private static readonly Type[] tinyTypes = { typeof(byte), typeof(short), typeof(int), typeof(long) };

		private static readonly String[] smallPrims = { "short", "int", "long" };
		private static readonly Type[] smallTypes = { typeof(short), typeof(int), typeof(long) };

		private static readonly String[] intPrims = { "int", "long" };
		private static readonly Type[] intTypes = { typeof(int), typeof(long) };

		private static readonly String[] bigPrims = { "long" };
		private static readonly Type[] bigTypes = { typeof(long) };

		private static readonly String[] floatPrims = { "float", "double" };
		private static readonly Type[] floatTypes = { typeof(float), typeof(double) };

		private static readonly String[] doublePrims = { "double" };
		private static readonly Type[] doubleTypes = { typeof(double) };

		private static readonly String[] stringPrims = { };
		private static readonly Type[] stringTypes = { typeof(string) };

		private static readonly String[] datePrims = { };
		private static readonly Type[] dateTypes = { typeof(DateTime) };

		/// <summary>
		/// Get the cost to cast an SQL type to the desired target types.
		/// </summary>
		/// <param name="arg">The argument to cast.</param>
		/// <param name="argSqlType">The SQL type of the arg.</param>
		/// <param name="target">The target to which to cast.</param>
		/// <remarks>
		/// The cost is 0 to cast to TObject, 100 to cast to the closest 
		/// primitive, or 200 to cast to the closest Object, plus 1 for 
		/// each widening away from the closest.
		/// </remarks>
		/// <returns>
		/// Returns the cost to do the cast, or -1 if the cast can not be done.
		/// </returns>
		static int GetCastingCost(TObject arg, SqlType argSqlType, Type target) {

			//If the user has a method that takes a TObject, assume he can handle
			//anything.
			if (target == typeof(TObject)) {
				return 0;
			}

			switch (argSqlType) {

				case SqlType.Bit:
					return GetCastingCost(arg, bitPrims, bitTypes, target);

				case SqlType.TinyInt:
					return GetCastingCost(arg, tinyPrims, tinyTypes, target);

				case SqlType.SmallInt:
					return GetCastingCost(arg, smallPrims, smallTypes, target);

				case SqlType.Integer:
					return GetCastingCost(arg, intPrims, intTypes, target);

				case SqlType.BigInt:
					return GetCastingCost(arg, bigPrims, bigTypes, target);

				case SqlType.Real:
					return GetCastingCost(arg, floatPrims, floatTypes, target);

				case SqlType.Float:
				case SqlType.Double:
					return GetCastingCost(arg, doublePrims, doubleTypes, target);

				// We only get a NUMERIC or DECIMAL type here if we were not able to
				// convert it to int, long or double, so we can't handle it.  For now we
				// require that these types be handled by a method that takes a TObject.
				// That gets checked at the top of this method, so if we get to here
				// the target is not a TOBject, so we don't know how to handle it.
				case SqlType.Numeric:
				case SqlType.Decimal:
					return -1;

				case SqlType.Char:
				case SqlType.VarChar:
				case SqlType.LongVarChar:
					return GetCastingCost(arg, stringPrims, stringTypes, target);

				case SqlType.Date:
				case SqlType.Time:
				case SqlType.TimeStamp:
					return GetCastingCost(arg, datePrims, dateTypes, target);

				case SqlType.Binary:
				case SqlType.VarBinary:
				case SqlType.LongVarBinary:
					return -1;	// Can't handle these, user must use TObject

				// We can cast a OBJECT only if the value is a subtype of the
				// target class.
				case SqlType.Object:
					Object argVal = arg.Object;
					if (argVal == null || target.IsAssignableFrom(argVal.GetType())) {
						return ObjectCost;
					}
					return -1;

				// If the declared data type is NULL, then we have no type info to
				// determine how to cast it.
				case SqlType.Null:
					return -1;

				default:
					return -1;	// Don't know how to cast other types
			}
		}

		/// <summary>
		/// Get the cost to cast to the specified target from the set of
		/// allowable primitives and object classes.
		/// </summary>
		/// <param name="arg">The value being cast.</param>
		/// <param name="prims">The set of allowable primitive types to which we can
		/// cast, ordered with the preferred types first. If the value of the arg is 
		/// null, it can not be cast to a primitive type.</param>
		/// <param name="objects">The set of allowable Object types to which we can
		/// cast, ordered with the preferred types first.</param>
		/// <param name="target">The target class to which we are casting.</param>
		/// <returns>
		/// Returns the cost of the cast, or -1 if the cast is not allowed.
		/// </returns>
		static int GetCastingCost(TObject arg, String[] prims, Type[] objects, Type target) {
			if (target.IsPrimitive) {
				Object argVal = arg.Object;	// get the vaue of the arg
				if (argVal == null) {
					return -1;	// can't cast null to a primitive
				}
				String targetName = target.Name;
				// Look for the closest allowable primitive
				for (int i = 0; i < prims.Length; i++) {
					if (targetName.Equals(prims[i]))
						return PrimitiveCost + i;
					// Cost of casting to a primitive plus the widening cost (i)
				}
			} else {
				// Look for the closest allowable object class
				for (int i = 0; i < objects.Length; i++) {
					if (objects[i].IsAssignableFrom(target))
						return ObjectCost + i;
					// Cost of casting to an object class plus the widening cost (i)
				}
			}
			return -1;	// can't cast it
		}

		///<summary>
		/// Cast the given arguments to the specified constructors parameter types.
		///</summary>
		///<param name="args">The database arguments from which to cast.</param>
		///<param name="constructor">The constructor to which to cast.</param>
		/// <remarks>
		/// The caller must already have checked to make sure the argument count
		/// and types match the constructor.
		/// </remarks>
		///<returns>
		/// Returns the cast arguments.
		/// </returns>
		public static Object[] CastArgsToConstructor(TObject[] args, ConstructorInfo constructor) {
			ParameterInfo[] parameters = constructor.GetParameters();
			Type[] targets = new Type[parameters.Length];
			for (int i = 0; i < parameters.Length; i++) {
				targets[i] = parameters[i].ParameterType;
			}
			return CastArgs(args, targets);
		}


		/// <summary>
		/// Cast the given arguments to the specified classes.
		/// </summary>
		/// <param name="args">The database arguments from which to cast.</param>
		/// <param name="targets">The types to which to cast.</param>
		/// <remarks>
		/// The caller must already have checked to make sure the argument count
		/// and types match the constructor.
		/// </remarks>
		/// <returns>
		/// Returns the cast arguments.
		/// </returns>
		static Object[] CastArgs(TObject[] args, Type[] targets) {
			if (targets.Length != args.Length) {
				// we shouldn't get this error
				throw new Exception("array length mismatch: arg=" + args.Length + ", targets=" + targets.Length);
			}
			Object[] castedArgs = new Object[args.Length];
			for (int n = 0; n < args.Length; ++n) {
				castedArgs[n] = CastArg(args[n], targets[n]);
			}
			return castedArgs;
		}

		public static bool IsNumber(object value) {
			if (value is sbyte) return true;
			if (value is byte) return true;
			if (value is short) return true;
			if (value is ushort) return true;
			if (value is int) return true;
			if (value is uint) return true;
			if (value is long) return true;
			if (value is ulong) return true;
			if (value is float) return true;
			if (value is double) return true;
			if (value is decimal) return true;
			return false;
		}

		/// <summary>
		/// Cast the object to the specified target.
		/// </summary>
		/// <param name="arg">The database argumument from which to cast.</param>
		/// <param name="target">The type to which to cast.</param>
		/// <returns>
		/// Returns the cast object.
		/// </returns>
		static Object CastArg(TObject arg, Type target) {
			// By the time we get here, we have already run through the cost function
			// and eliminated the casts that don't work, including not allowing a null
			// value to be cast to a primitive type.

			if (target == typeof(TObject)) {
				return arg;
			}

			Object argVal = arg.Object;
			if (argVal == null) {
				// If the arg is null, then we must be casting to an Object type,
				// so just return null.
				return null;
			}

			//boolean isPrimitive = target.isPrimitive();
			String targetName = target.Name;

			if (argVal is Boolean) {
				//Bit
				if (targetName.Equals("boolean") ||
					  typeof(Boolean).IsAssignableFrom(target)) {
					return argVal;
				}
			} else if (IsNumber(argVal)) {
				//TINYINT, SMALLINT, INTEGER, BIGINT,
				//REAL, FLOAT, DOUBLE, NUMERIC, DECIMAL
				if (targetName.Equals("byte") || typeof(Byte).IsAssignableFrom(target)) {
					return Convert.ToByte(argVal);
				}
				if (targetName.Equals("short") || typeof(short).IsAssignableFrom(target)) {
					return Convert.ToInt16(argVal);
				}
				if (targetName.Equals("int") || typeof(int).IsAssignableFrom(target)) {
					return Convert.ToInt32(argVal);
				}
				if (targetName.Equals("long") || typeof(long).IsAssignableFrom(target)) {
					return Convert.ToInt64(argVal);
				}
				if (targetName.Equals("float") || typeof(float).IsAssignableFrom(target)) {
					return Convert.ToSingle(argVal);
				}
				if (targetName.Equals("double") || typeof(double).IsAssignableFrom(target)) {
					return Convert.ToDouble(argVal);
				}
			} else if (argVal is DateTime) {
				//DATE, TIME, TIMESTAMP
				DateTime date = (DateTime)argVal;
				if (typeof(DateTime).IsAssignableFrom(target)) {
					return date;
				}
			} else if (argVal is String ||
					   argVal is StringObject) {
				//CHAR, VARCHAR, LONGVARCHAR
				String s = argVal.ToString();
				if (typeof(string).IsAssignableFrom(target)) {
					return s;
				}
			} else if (GetSqlType(arg) == SqlType.Object) {
				// OBJECT
				if (target.IsAssignableFrom(argVal.GetType())) {
					return argVal;
				}
			} else {
				// BINARY, VARBINARY, LONGVARBINARY
				// NULL
				// We don't know how to handle any of these except as TObject
			}

			// Can't cast - we should not get here, since we checked for the
			// legality of the cast when calculating the cost.  However, the
			// code to do the cost is not the same as the code to do the casting,
			// so we may have messed up in one or the other.

			throw new Exception("Programming error: Can't cast from " +
							argVal.GetType().Name + " to " + target.Name);
		}
	}
}