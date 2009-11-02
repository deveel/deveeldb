//  
//  ArithmeticFunctionFactory.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
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

using Deveel.Math;

namespace Deveel.Data.Functions {
	internal class ArithmeticFunctionFactory : FunctionFactory {
		public override void Init() {
			AddFunction("abs", typeof(AbsFunction));
			AddFunction("acos", typeof(ACosFunction));
			AddFunction("asin", typeof(ASinFunction));
			AddFunction("atan", typeof(ATanFunction));
			AddFunction("cos", typeof(CosFunction));
			AddFunction("cosh", typeof(CosHFunction));
			AddFunction("sign", typeof(SignFunction));
			AddFunction("signum", typeof(SignFunction));
			AddFunction("sin", typeof(SinFunction));
			AddFunction("sinh", typeof(SinHFunction));
			AddFunction("sqrt", typeof(SqrtFunction));
			AddFunction("tan", typeof(TanFunction));
			AddFunction("tanh", typeof(TanHFunction));
			AddFunction("mod", typeof(ModFunction));
			AddFunction("pow", typeof(PowFunction));
			AddFunction("round", typeof(RoundFunction));
		}

		#region AbsFunction

		[Serializable]
		internal sealed class AbsFunction : Function {
			public AbsFunction(Expression[] parameters)
				: base("abs", parameters) {

				if (ParameterCount != 1) {
					throw new Exception("Abs function must have one argument.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull) {
					return ob;
				}
				BigNumber num = ob.ToBigNumber();
				return TObject.GetBigNumber(num.Abs());
			}

		}

		#endregion

		#region ACosFunction

		[Serializable]
		sealed class ACosFunction : Function {
			public ACosFunction(Expression[] parameters)
				: base("acos", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				if (ob.TType is TNumericType)
					ob = ob.CastTo(TType.NumericType);

				return TObject.GetBigNumber(BigNumber.fromDouble(System.Math.Acos(ob.ToBigNumber().ToDouble())));
			}
		}

		#endregion

		#region ASinFunction

		[Serializable]
		public sealed class ASinFunction : Function {
			public ASinFunction(Expression[] parameters)
				: base("asin", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				if (ob.TType is TNumericType)
					ob = ob.CastTo(TType.NumericType);

				return TObject.GetBigNumber(BigNumber.fromDouble(System.Math.Asin(ob.ToBigNumber().ToDouble())));
			}
		}

		#endregion

		#region ATanFunction

		[Serializable]
		sealed class ATanFunction : Function {
			public ATanFunction(Expression[] parameters)
				: base("atan", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				if (ob.TType is TNumericType)
					ob = ob.CastTo(TType.NumericType);

				return TObject.GetBigNumber(BigNumber.fromDouble(System.Math.Atan(ob.ToBigNumber().ToDouble())));
			}
		}


		#endregion

		#region CosFunction

		[Serializable]
		sealed class CosFunction : Function {
			public CosFunction(Expression[] parameters)
				: base("cos", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				if (ob.TType is TNumericType)
					ob = ob.CastTo(TType.NumericType);

				return TObject.GetBigNumber(BigNumber.fromDouble(System.Math.Cos(ob.ToBigNumber().ToDouble())));
			}
		}


		#endregion

		#region CosHFunction

		[Serializable]
		sealed class CosHFunction : Function {
			public CosHFunction(Expression[] parameters)
				: base("cosh", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				if (ob.TType is TNumericType)
					ob = ob.CastTo(TType.NumericType);

				return TObject.GetBigNumber(BigNumber.fromDouble(System.Math.Cosh(ob.ToBigNumber().ToDouble())));
			}
		}


		#endregion

		#region SignFunction

		[Serializable]
		sealed class SignFunction : Function {
			public SignFunction(Expression[] parameters)
				: base("sign", parameters) {

				if (ParameterCount != 1) {
					throw new Exception("Sign function must have one argument.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull) {
					return ob;
				}
				BigNumber num = ob.ToBigNumber();
				return TObject.GetInt4(num.Signum());
			}
		}

		#endregion

		#region SinFunction

		[Serializable]
		class SinFunction : Function {
			public SinFunction(Expression[] parameters)
				: base("sin", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				if (ob.TType is TNumericType)
					ob = ob.CastTo(TType.NumericType);

				return TObject.GetBigNumber(BigNumber.fromDouble(System.Math.Sin(ob.ToBigNumber().ToDouble())));
			}
		}

		#endregion

		#region SinHFunction

		[Serializable]
		class SinHFunction : Function {
			public SinHFunction(Expression[] parameters)
				: base("sinh", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				if (ob.TType is TNumericType)
					ob = ob.CastTo(TType.NumericType);

				return TObject.GetBigNumber(BigNumber.fromDouble(System.Math.Sinh(ob.ToBigNumber().ToDouble())));
			}
		}

		#endregion

		#region SqrtFunction

		[Serializable]
		class SqrtFunction : Function {
			public SqrtFunction(Expression[] parameters)
				: base("sqrt", parameters) {

				if (ParameterCount != 1) {
					throw new Exception("Sqrt function must have one argument.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull) {
					return ob;
				}

				return TObject.GetBigNumber(ob.ToBigNumber().Sqrt());
			}
		}

		#endregion

		#region TanFunction

		[Serializable]
		class TanFunction : Function {
			public TanFunction(Expression[] parameters)
				: base("tan", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				if (ob.TType is TNumericType)
					ob = ob.CastTo(TType.NumericType);

				return TObject.GetBigNumber(Math.BigNumber.fromDouble(System.Math.Tan(ob.ToBigNumber().ToDouble())));
			}
		}


		#endregion

		#region TanHFunction

		[Serializable]
		class TanHFunction : Function {
			public TanHFunction(Expression[] parameters)
				: base("tanh", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				if (ob.TType is TNumericType)
					ob = ob.CastTo(TType.NumericType);

				return TObject.GetBigNumber(Math.BigNumber.fromDouble(System.Math.Tanh(ob.ToBigNumber().ToDouble())));
			}
		}


		#endregion

		#region ModFunction

		[Serializable]
		class ModFunction : Function {
			public ModFunction(Expression[] parameters)
				: base("mod", parameters) {

				if (ParameterCount != 2)
					throw new Exception("Mod function must have two arguments.");
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver,
											 IQueryContext context) {
				TObject ob1 = this[0].Evaluate(group, resolver, context);
				TObject ob2 = this[1].Evaluate(group, resolver, context);
				if (ob1.IsNull) {
					return ob1;
				} else if (ob2.IsNull) {
					return ob2;
				}

				double v = ob1.ToBigNumber().ToDouble();
				double m = ob2.ToBigNumber().ToDouble();
				return TObject.GetDouble(v % m);
			}
		}


		#endregion

		#region PowFunction

		[Serializable]
		class PowFunction : Function {
			public PowFunction(Expression[] parameters)
				: base("pow", parameters) {

				if (ParameterCount != 2) {
					throw new Exception("Pow function must have two arguments.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver,
											 IQueryContext context) {
				TObject ob1 = this[0].Evaluate(group, resolver, context);
				TObject ob2 = this[1].Evaluate(group, resolver, context);
				if (ob1.IsNull) {
					return ob1;
				} else if (ob2.IsNull) {
					return ob2;
				}

				double v = ob1.ToBigNumber().ToDouble();
				double w = ob2.ToBigNumber().ToDouble();
				return TObject.GetDouble(System.Math.Pow(v, w));
			}
		}

		#endregion

		#region RoundFunction

		[Serializable]
		class RoundFunction : Function {
			public RoundFunction(Expression[] parameters)
				: base("round", parameters) {

				if (ParameterCount < 1 || ParameterCount > 2) {
					throw new Exception("Round function must have one or two arguments.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob1 = this[0].Evaluate(group, resolver, context);
				if (ob1.IsNull) {
					return ob1;
				}

				BigNumber v = ob1.ToBigNumber();
				int d = 0;
				if (ParameterCount == 2) {
					TObject ob2 = this[1].Evaluate(group, resolver, context);
					if (ob2.IsNull) {
						d = 0;
					} else {
						d = ob2.ToBigNumber().ToInt32();
					}
				}
				return TObject.GetBigNumber(v.SetScale(d, DecimalRoundingMode.HalfUp));
			}
		}

		#endregion
	}
}