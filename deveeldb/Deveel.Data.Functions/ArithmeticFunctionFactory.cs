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
			AddFunction("log", typeof(LogFunction));
			AddFunction("log10", typeof(Log10Function));
			AddFunction("pi", typeof(PiFunction));
			AddFunction("e", typeof(EFunction));
			AddFunction("ceil", typeof(CeilFunction));
			AddFunction("ceiling", typeof(CeilFunction));
			AddFunction("floor", typeof(FloorFunction));
			AddFunction("radians", typeof(RadiansFunction));
			AddFunction("degrees", typeof(DegreesFunction));
			AddFunction("exp", typeof(ExpFunction));
			AddFunction("cot", typeof (CotFunction));
			AddFunction("arctan", typeof(ArcTanFunction));
			AddFunction("rand", typeof(RandFunction));
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

				return TObject.GetBigNumber(System.Math.Acos(ob.ToBigNumber().ToDouble()));
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

				return TObject.GetBigNumber(System.Math.Asin(ob.ToBigNumber().ToDouble()));
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

				return TObject.GetBigNumber(System.Math.Atan(ob.ToBigNumber().ToDouble()));
			}
		}


		#endregion

		#region ArcTanFunction

		[Serializable]
		private class ArcTanFunction : Function {
			public ArcTanFunction(Expression[] parameters) 
				: base("arctan", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return TObject.Null;

				double degrees = ob.ToBigNumber().ToDouble();
				double radians = RadiansFunction.ToRadians(degrees);
				radians = System.Math.Tan(System.Math.Atan(radians));
				degrees = DegreesFunction.ToDegrees(radians);

				return TObject.GetBigNumber(degrees);
			}
		}

		#endregion

		#region CotFunction

		[Serializable]
		private class CotFunction : Function {
			public CotFunction(Expression[] parameters) 
				: base("cot", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return TObject.Null;

				double degrees = ob.ToBigNumber().ToDouble();
				double radians = RadiansFunction.ToRadians(degrees);
				double cotan = 1.0/System.Math.Tan(radians);

				return TObject.GetBigNumber(cotan);
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

				return TObject.GetBigNumber(System.Math.Cos(ob.ToBigNumber().ToDouble()));
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

				return TObject.GetBigNumber(System.Math.Cosh(ob.ToBigNumber().ToDouble()));
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

				return TObject.GetBigNumber(System.Math.Sin(ob.ToBigNumber().ToDouble()));
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

				return TObject.GetBigNumber(System.Math.Sinh(ob.ToBigNumber().ToDouble()));
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

				return TObject.GetBigNumber(System.Math.Tan(ob.ToBigNumber().ToDouble()));
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

				return TObject.GetBigNumber(System.Math.Tanh(ob.ToBigNumber().ToDouble()));
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

		#region LogFunction

		[Serializable]
		private class LogFunction : Function {
			public LogFunction(Expression[] parameters) 
				: base("log", parameters) {
				if (ParameterCount > 2)
					throw new ArgumentException("The LOG function accepts 1 or 2 arguments.");
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				int argc = ParameterCount;

				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				if (ob.TType is TNumericType)
					ob = ob.CastTo(TType.NumericType);

				double a = ob.ToBigNumber().ToDouble();
				double newBase = double.NaN;
				if (argc == 2) {
					TObject ob1 = this[1].Evaluate(group, resolver, context);
					if (!ob1.IsNull) {
						if (ob1.TType is TNumericType)
							ob1 = ob.CastTo(TType.NumericType);

						newBase = ob1.ToBigNumber().ToDouble();
					}
				}

				double result = (argc == 1 ? System.Math.Log(a) : System.Math.Log(a, newBase));
				return TObject.GetBigNumber(result);
			}
		}

		#endregion

		#region Log10Function

		[Serializable]
		private class Log10Function : Function {
			public Log10Function(Expression[] parameters) 
				: base("log10", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				if (ob.TType is TNumericType)
					ob = ob.CastTo(TType.NumericType);

				return TObject.GetBigNumber(System.Math.Log10(ob.ToBigNumber().ToDouble()));
			}
		}

		#endregion

		#region PiFunction

		[Serializable]
		private class PiFunction : Function {
			public PiFunction(Expression[] parameters) 
				: base("pi", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				return TObject.GetBigNumber(System.Math.PI);
			}
		}

		#endregion

		#region EFunction

		[Serializable]
		private class EFunction : Function {
			public EFunction(Expression[] parameters) 
				: base("e", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				return TObject.GetBigNumber(System.Math.E);
			}
		}

		#endregion

		#region CeilFunction

		[Serializable]
		private class CeilFunction : Function {
			public CeilFunction(Expression[] parameters) 
				: base("ceil", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return TObject.Null;

				if (!(ob.TType is TNumericType))
					ob = ob.CastTo(TType.NumericType);

				return TObject.GetBigNumber(ob.ToBigNumber().ToDouble());
			}
		}

		#endregion

		#region FloorFunction

		[Serializable]
		private class FloorFunction : Function {
			public FloorFunction(Expression[] parameters) 
				: base("floor", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return TObject.Null;

				if (!(ob.TType is TNumericType))
					ob = ob.CastTo(TType.NumericType);

				return TObject.GetBigNumber(System.Math.Floor(ob.ToBigNumber().ToDouble()));
			}
		}

		#endregion

		#region RadiansFunction

		[Serializable]
		private class RadiansFunction : Function {
			public RadiansFunction(Expression[] parameters) 
				: base("radians", parameters) {
			}

			/// <summary>
			/// The number of radians for one degree.
			/// </summary>
			private const double Degree = 0.0174532925;

			internal static double ToRadians(double degrees) {
				return degrees * Degree;
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return TObject.Null;

				double degrees = ob.ToBigNumber().ToDouble();
				double radians = ToRadians(degrees);

				return TObject.GetBigNumber(radians);
			}
		}

		#endregion

		#region DegreesFunction

		[Serializable]
		private class DegreesFunction : Function {
			public DegreesFunction(Expression[] parameters) 
				: base("degrees", parameters) {
			}

			/// <summary>
			/// The number of degrees for one radiant.
			/// </summary>
			private const double Radiant = 57.2957795;

			internal static double ToDegrees(double radians) {
				return radians*Radiant;
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return TObject.Null;

				double radians = ob.ToBigNumber().ToDouble();
				double degrees = ToDegrees(radians);

				return TObject.GetBigNumber(degrees);
			}
		}

		#endregion

		#region ExpFunction

		[Serializable]
		private class ExpFunction : Function {
			public ExpFunction(Expression[] parameters) 
				: base("exp", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return TObject.Null;

				if (!(ob.TType is TNumericType))
					ob = ob.CastTo(TType.NumericType);

				return TObject.GetBigNumber(System.Math.Exp(ob.ToBigNumber().ToDouble()));
			}
		}

		#endregion

		#region RandFunction

		[Serializable]
		private class RandFunction : Function {
			public RandFunction(Expression[] parameters) 
				: base("rand", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				int argc = ParameterCount;

				// TODO: should we initialize at higher level to keep the state?

				Random random;
				if (argc == 1) {
					TObject ob = this[0].Evaluate(group, resolver, context);
					if (!ob.IsNull)
						random = new Random(ob.ToBigNumber().ToInt32());
					else
						random = new Random();
				} else {
					random = new Random();
				}

				double value = random.NextDouble();
				return TObject.GetBigNumber(value);
			}
		}

		#endregion
	}
}