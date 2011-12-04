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
using System.Globalization;
using System.Text;

using Deveel.Data.Text;

namespace Deveel.Data.Functions {
	internal class StringFunctionFactory : FunctionFactory {
		public override void Init() {
			AddFunction("concat", typeof(ConcatFunction));
			AddFunction("lower", typeof(LowerFunction));
			AddFunction("tolower", typeof(LowerFunction));
			AddFunction("upper", typeof(UpperFunction));
			AddFunction("toupper", typeof(UpperFunction));
			AddFunction("sql_trim", typeof(SQLTrimFunction));
			AddFunction("ltrim", typeof(LTrimFunction));
			AddFunction("rtrim", typeof(RTrimFunction));
			AddFunction("substring", typeof(SubstringFunction));
			AddFunction("instr", typeof (InStrFunction));
			AddFunction("soundex", typeof(SoundexFunction));
			AddFunction("lpad", typeof(LPadFunction));
			AddFunction("rpad", typeof(RPadFunction));
			AddFunction("replace", typeof(ReplaceFunction));
			AddFunction("char_length", typeof(CharLengthFunction));
			AddFunction("character_length", typeof(CharLengthFunction));
			AddFunction("octet_length", typeof(OctetLengthFunction));
		}

		#region ConcatFunction

		[Serializable]
		class ConcatFunction : Function {

			public ConcatFunction(Expression[] parameters)
				: base("concat", parameters) {

				if (ParameterCount < 1) {
					throw new Exception("Concat function must have at least one argument.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				StringBuilder cc = new StringBuilder();

				CultureInfo str_locale = null;
				CollationStrength str_strength = 0;
				CollationDecomposition str_decomposition = 0;
				for (int i = 0; i < ParameterCount; ++i) {
					Expression cur_parameter = this[i];
					TObject ob = cur_parameter.Evaluate(group, resolver, context);
					if (!ob.IsNull) {
						cc.Append(ob.Object.ToString());
						TType type1 = ob.TType;
						if (str_locale == null && type1 is TStringType) {
							TStringType str_type = (TStringType)type1;
							str_locale = str_type.Locale;
							str_strength = str_type.Strength;
							str_decomposition = str_type.Decomposition;
						}
					} else {
						return ob;
					}
				}

				// We inherit the locale from the first string parameter with a locale,
				// or use a default StringType if no locale found.
				TType type;
				if (str_locale != null) {
					type = new TStringType(SqlType.VarChar, -1,
										   str_locale, str_strength, str_decomposition);
				} else {
					type = TType.StringType;
				}

				return new TObject(type, cc.ToString());
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				// Determine the locale of the first string parameter.
				CultureInfo str_locale = null;
				CollationStrength str_strength = 0;
				CollationDecomposition str_decomposition = 0;
				for (int i = 0; i < ParameterCount && str_locale == null; ++i) {
					TType type = this[i].ReturnTType(resolver, context);
					if (type is TStringType) {
						TStringType str_type = (TStringType)type;
						str_locale = str_type.Locale;
						str_strength = str_type.Strength;
						str_decomposition = str_type.Decomposition;
					}
				}

				if (str_locale != null) {
					return new TStringType(SqlType.VarChar, -1,
										   str_locale, str_strength, str_decomposition);
				} else {
					return TType.StringType;
				}
			}

		}

		#endregion

		#region LowerFunction

		[Serializable]
		class LowerFunction : Function {
			public LowerFunction(Expression[] parameters)
				: base("lower", parameters) {

				if (ParameterCount != 1) {
					throw new Exception("Lower function must have one argument.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull) {
					return ob;
				}
				return new TObject(ob.TType, ob.Object.ToString().ToLower());
			}

			protected override TType ReturnTType() {
				return TType.StringType;
			}
		}

		#endregion

		#region UpperFunction

		[Serializable]
		class UpperFunction : Function {
			public UpperFunction(Expression[] parameters)
				: base("upper", parameters) {

				if (ParameterCount != 1)
					throw new Exception("Upper function must have one argument.");
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull) {
					return ob;
				}
				return new TObject(ob.TType, ob.Object.ToString().ToUpper());
			}

			protected override TType ReturnTType() {
				return TType.StringType;
			}

		}

		#endregion

		#region SQLTrimFunction

		[Serializable]
		class SQLTrimFunction : Function {

			public SQLTrimFunction(Expression[] parameters)
				: base("sql_trim", parameters) {

				//      Console.Out.WriteLine(parameterCount());
				if (ParameterCount != 3) {
					throw new Exception(
						"SQL Trim function must have three parameters.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				// The type of trim (leading, both, trailing)
				TObject ttype = this[0].Evaluate(group, resolver, context);
				// Characters to trim
				TObject cob = this[1].Evaluate(group, resolver, context);
				if (cob.IsNull) {
					return cob;
				} else if (ttype.IsNull) {
					return TObject.CreateString((StringObject)null);
				}
				String characters = cob.Object.ToString();
				String ttype_str = ttype.Object.ToString();
				// The content to trim.
				TObject ob = this[2].Evaluate(group, resolver, context);
				if (ob.IsNull) {
					return ob;
				}
				String str = ob.Object.ToString();

				int skip = characters.Length;
				// Do the trim,
				if (ttype_str.Equals("leading") || ttype_str.Equals("both")) {
					// Trim from the start.
					int scan = 0;
					while (scan < str.Length &&
						   str.IndexOf(characters, scan) == scan) {
						scan += skip;
					}
					str = str.Substring(System.Math.Min(scan, str.Length));
				}
				if (ttype_str.Equals("trailing") || ttype_str.Equals("both")) {
					// Trim from the end.
					int scan = str.Length - 1;
					int i = str.LastIndexOf(characters, scan);
					while (scan >= 0 && i != -1 && i == scan - skip + 1) {
						scan -= skip;
						i = str.LastIndexOf(characters, scan);
					}
					str = str.Substring(0, System.Math.Max(0, scan + 1));
				}

				return TObject.CreateString(str);
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return TType.StringType;
			}

		}

		#endregion

		#region LTrimFunction

		[Serializable]
		sealed class LTrimFunction : Function {
			public LTrimFunction(Expression[] parameters)
				: base("ltrim", parameters) {

				if (ParameterCount != 1)
					throw new Exception("ltrim function may only have 1 parameter.");
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver,
											 IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull) {
					return ob;
				}
				String str = ob.Object.ToString();

				// Do the trim,
				// Trim from the start.
				int scan = 0;
				while (scan < str.Length &&
					   str.IndexOf(' ', scan) == scan) {
					scan += 1;
				}
				str = str.Substring(System.Math.Min(scan, str.Length));

				return TObject.CreateString(str);
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return TType.StringType;
			}

		}

		#endregion

		#region RTrimFunction

		[Serializable]
		class RTrimFunction : Function {

			public RTrimFunction(Expression[] parameters)
				: base("rtrim", parameters) {

				if (ParameterCount != 1) {
					throw new Exception("rtrim function may only have 1 parameter.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver,
											 IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull) {
					return ob;
				}
				String str = ob.Object.ToString();

				// Do the trim,
				// Trim from the end.
				int scan = str.Length - 1;
				int i = str.LastIndexOf(" ", scan);
				while (scan >= 0 && i != -1 && i == scan - 2) {
					scan -= 1;
					i = str.LastIndexOf(" ", scan);
				}
				str = str.Substring(0, System.Math.Max(0, scan + 1));

				return TObject.CreateString(str);
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return TType.StringType;
			}

		}

		#endregion

		#region SubstringFunction

		[Serializable]
		class SubstringFunction : Function {
			public SubstringFunction(Expression[] parameters)
				: base("substring", parameters) {

				if (ParameterCount < 1 || ParameterCount > 3) {
					throw new Exception("Substring function needs one to three arguments.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull) {
					return ob;
				}
				String str = ob.Object.ToString();
				int pcount = ParameterCount;
				int str_length = str.Length;
				int arg1 = 1;
				int arg2 = str_length;
				if (pcount >= 2) {
					arg1 = this[1].Evaluate(group, resolver, context).ToBigNumber().ToInt32();
				}
				if (pcount >= 3) {
					arg2 = this[2].Evaluate(group, resolver, context).ToBigNumber().ToInt32();
				}

				// Make sure this call is safe for all lengths of string.
				if (arg1 < 1) {
					arg1 = 1;
				}
				if (arg1 > str_length) {
					return TObject.CreateString("");
				}
				if (arg2 + arg1 > str_length) {
					arg2 = (str_length - arg1) + 1;
				}
				if (arg2 < 1) {
					return TObject.CreateString("");
				}

				//TODO: check this...
				return TObject.CreateString(str.Substring(arg1 - 1, (arg1 + arg2) - 1));
			}

			protected override TType ReturnTType() {
				return TType.StringType;
			}

		}

		#endregion

		#region InStrFunction

		[Serializable]
		private class InStrFunction : Function {
			public InStrFunction(Expression[] parameters) 
				: base("instr", parameters) {
				if (ParameterCount < 2 || ParameterCount > 4)
					throw new ArgumentException("The function INSTR must specify at least 2 and less than 4 parameters.");
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				int argc = ParameterCount;

				TObject ob1 = this[0].Evaluate(group, resolver, context);
				TObject ob2 = this[1].Evaluate(group, resolver, context);

				if (ob1.IsNull)
					return TObject.Null;

				if (ob2.IsNull)
					return TObject.CreateInt4(-1);

				string str = ob1.Object.ToString();
				string pattern = ob2.Object.ToString();

				if (str.Length == 0 || pattern.Length == 0)
					return TObject.CreateInt4(-1);

				int startIndex = -1;
				int endIndex = -1;

				if (argc > 2) {
					TObject ob3 = this[2].Evaluate(group, resolver, context);
					if (!ob3.IsNull)
						startIndex = ob3.ToBigNumber().ToInt32();
				} 
				if (argc > 3) {
					TObject ob4 = this[3].Evaluate(group, resolver, context);
					if (!ob4.IsNull)
						endIndex = ob4.ToBigNumber().ToInt32();
				}

				int index = -1;
				if (argc == 2) {
					index = str.IndexOf(pattern);
				} else if (argc == 3) {
					index = str.IndexOf(pattern, startIndex);
				} else {
					index = str.IndexOf(pattern, startIndex, endIndex - startIndex);
				}

				return TObject.CreateInt4(index);
			}
		}

		#endregion

		#region SoundexFunction

		[Serializable]
		class SoundexFunction : Function {
			public SoundexFunction(Expression[] parameters)
				: base("soundex", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject obj = this[0].Evaluate(group, resolver, context);

				if (!(obj.TType is TStringType))
					obj = obj.CastTo(TType.StringType);

				return TObject.CreateString(Soundex.UsEnglish.Compute(obj.ToStringValue()));
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return TType.StringType;
			}
		}

		#endregion

		#region LPadFunction

		[Serializable]
		private class LPadFunction : Function {
			public LPadFunction(Expression[] parameters) 
				: base("lpad", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				int argc = ParameterCount;

				TObject ob1 = this[0].Evaluate(group, resolver, context);
				TObject ob2 = this[1].Evaluate(group, resolver, context);

				if (ob1.IsNull)
					return ob1;

				char c = ' ';
				if (argc > 2) {
					TObject ob3 = this[2].Evaluate(group, resolver, context);
					if (!ob3.IsNull) {
						string pad = ob3.ToStringValue();
						c = pad[0];
					}
				}

				int totalWidth = ob2.ToBigNumber().ToInt32();
				string s = ob1.ToStringValue();

				string result = (argc == 1 ? s.PadLeft(totalWidth) : s.PadLeft(totalWidth, c));
				return TObject.CreateString(result);
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return TType.StringType;
			}
		}

		#endregion

		#region RPadFunction

		[Serializable]
		private class RPadFunction : Function {
			public RPadFunction(Expression[] parameters) 
				: base("rpad", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				int argc = ParameterCount;

				TObject ob1 = this[0].Evaluate(group, resolver, context);
				TObject ob2 = this[1].Evaluate(group, resolver, context);

				if (ob1.IsNull)
					return ob1;

				char c = ' ';
				if (argc > 2) {
					TObject ob3 = this[2].Evaluate(group, resolver, context);
					if (!ob3.IsNull) {
						string pad = ob3.ToStringValue();
						c = pad[0];
					}
				}

				int totalWidth = ob2.ToBigNumber().ToInt32();
				string s = ob1.ToStringValue();

				string result = (argc == 1 ? s.PadRight(totalWidth) : s.PadRight(totalWidth, c));
				return TObject.CreateString(result);
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return TType.StringType;
			}
		}

		#endregion

		#region ReplaceFunction

		[Serializable]
		private class ReplaceFunction : Function {
			public ReplaceFunction(Expression[] parameters) 
				: base("replace", parameters) {
				if (ParameterCount != 3)
					throw new ArgumentException("The function REPLACE requires 3 parameters.");
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob1 = this[0].Evaluate(group, resolver, context);
				TObject ob2 = this[1].Evaluate(group, resolver, context);
				TObject ob3 = this[2].Evaluate(group, resolver, context);

				if (ob1.IsNull)
					return ob1;

				if (ob2.IsNull)
					return ob1;

				string s = ob1.ToStringValue();
				string oldValue = ob2.ToStringValue();
				string newValue = ob3.ToStringValue();

				string result = s.Replace(oldValue, newValue);
				return TObject.CreateString(result);
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return TType.StringType;
			}
		}

		#endregion

		#region CharLengthFunction

		[Serializable]
		private class CharLengthFunction : Function {
			public CharLengthFunction(Expression[] parameters) 
				: base("char_length", parameters) {
			}

			#region Overrides of Function

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (!(ob.TType is TStringType) || ob.IsNull)
					return TObject.Null;

				IStringAccessor s = (IStringAccessor)ob.Object;
				if (s == null)
					return TObject.Null;

				return (TObject) s.Length;
			}

			#endregion
		}

		#endregion

		#region OctetLengthFunction

		[Serializable]
		private class OctetLengthFunction : Function {
			public OctetLengthFunction(Expression[] parameters) 
				: base("octet_length", parameters) {
			}

			#region Overrides of Function

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (!(ob.TType is TStringType) || ob.IsNull)
					return TObject.Null;

				IStringAccessor s = (IStringAccessor)ob.Object;
				if (s == null)
					return TObject.Null;

				// by default a character is an UNICODE, which requires 
				// two bytes...
				long size = s.Length * 2;
				if (s is IRef)
					size = (s as IRef).RawSize;

				return (TObject) size;
			}

			#endregion
		}

		#endregion
	}
}