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

using Deveel.Data.DbSystem;
using Deveel.Data.Types;
using Deveel.Math;

using SysMath = System.Math;

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
				return TObject.CreateBigNumber(num.Abs());
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
					ob = ob.CastTo(PrimitiveTypes.Numeric);

				return TObject.CreateBigNumber(SysMath.Acos(ob.ToBigNumber().ToDouble()));
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
					ob = ob.CastTo(PrimitiveTypes.Numeric);

				return TObject.CreateBigNumber(SysMath.Asin(ob.ToBigNumber().ToDouble()));
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
					ob = ob.CastTo(PrimitiveTypes.Numeric);

				return TObject.CreateBigNumber(SysMath.Atan(ob.ToBigNumber().ToDouble()));
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
				radians = SysMath.Tan(SysMath.Atan(radians));
				degrees = DegreesFunction.ToDegrees(radians);

				return TObject.CreateBigNumber(degrees);
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
				double cotan = 1.0/SysMath.Tan(radians);

				return TObject.CreateBigNumber(cotan);
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
					ob = ob.CastTo(PrimitiveTypes.Numeric);

				return TObject.CreateBigNumber(SysMath.Cos(ob.ToBigNumber().ToDouble()));
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
					ob = ob.CastTo(PrimitiveTypes.Numeric);

				return TObject.CreateBigNumber(SysMath.Cosh(ob.ToBigNumber().ToDouble()));
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
				return TObject.CreateInt4(num.Signum());
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
					ob = ob.CastTo(PrimitiveTypes.Numeric);

				return TObject.CreateBigNumber(SysMath.Sin(ob.ToBigNumber().ToDouble()));
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
					ob = ob.CastTo(PrimitiveTypes.Numeric);

				return TObject.CreateBigNumber(SysMath.Sinh(ob.ToBigNumber().ToDouble()));
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

				return TObject.CreateBigNumber(ob.ToBigNumber().Sqrt());
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
					ob = ob.CastTo(PrimitiveTypes.Numeric);

				return TObject.CreateBigNumber(SysMath.Tan(ob.ToBigNumber().ToDouble()));
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
					ob = ob.CastTo(PrimitiveTypes.Numeric);

				return TObject.CreateBigNumber(SysMath.Tanh(ob.ToBigNumber().ToDouble()));
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
				return TObject.CreateDouble(v % m);
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
				return TObject.CreateDouble(SysMath.Pow(v, w));
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
				return TObject.CreateBigNumber(v.SetScale(d, RoundingMode.HalfUp));
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
					ob = ob.CastTo(PrimitiveTypes.Numeric);

				double a = ob.ToBigNumber().ToDouble();
				double newBase = double.NaN;
				if (argc == 2) {
					TObject ob1 = this[1].Evaluate(group, resolver, context);
					if (!ob1.IsNull) {
						if (ob1.TType is TNumericType)
							ob1 = ob.CastTo(PrimitiveTypes.Numeric);

						newBase = ob1.ToBigNumber().ToDouble();
					}
				}

				double result = (argc == 1 ? SysMath.Log(a) : SysMath.Log(a, newBase));
				return TObject.CreateBigNumber(result);
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
					ob = ob.CastTo(PrimitiveTypes.Numeric);

				return TObject.CreateBigNumber(SysMath.Log10(ob.ToBigNumber().ToDouble()));
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
				return TObject.CreateBigNumber(SysMath.PI);
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
				return TObject.CreateBigNumber(SysMath.E);
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
					ob = ob.CastTo(PrimitiveTypes.Numeric);

				return TObject.CreateBigNumber(ob.ToBigNumber().ToDouble());
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
					ob = ob.CastTo(PrimitiveTypes.Numeric);

				return TObject.CreateBigNumber(SysMath.Floor(ob.ToBigNumber().ToDouble()));
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

				return TObject.CreateBigNumber(radians);
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

				return TObject.CreateBigNumber(degrees);
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
					ob = ob.CastTo(PrimitiveTypes.Numeric);

				return TObject.CreateBigNumber(SysMath.Exp(ob.ToBigNumber().ToDouble()));
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
				return TObject.CreateBigNumber(value);
			}
		}

		#endregion
	}
}