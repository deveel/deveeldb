// 
//  Copyright 2010-2014 Deveel
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
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;

using Deveel.Data.DbSystem;
using Deveel.Data.Types;

namespace Deveel.Data.Routines {
	public abstract class FunctionFactory : IRoutineResolver {
		private readonly Dictionary<FunctionInfo, object> functionTypeMapping;
		private readonly Dictionary<string, string> aliases;
 
		private bool initd;

		protected FunctionFactory() {
			functionTypeMapping = new Dictionary<FunctionInfo, object>();
			aliases = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);

			GlobExpression = new Expression();
			GlobExpression.AddElement(TObject.CreateString("*"));
			GlobExpression.Text.Append("*");

			GlobList = new Expression[] { GlobExpression };
		}

		private static readonly char[] Alpha = "abcdefghkjilmnopqrstuvwxyz".ToCharArray();

		public static Expression GlobExpression { get; private set; }

		public static Expression[] GlobList { get; private set; }

		protected void Alias(string functionName, string alias) {
			if (!functionTypeMapping.Any(x => String.Equals(x.Key.Name.Name, functionName, StringComparison.OrdinalIgnoreCase)))
				throw new ArgumentException(String.Format("Function {0} was not defined by this factory.", functionName));

			if (functionTypeMapping.Any(x => String.Equals(x.Key.Name.Name, alias, StringComparison.OrdinalIgnoreCase)))
				throw new InvalidOperationException(String.Format("A function named '{0}' is already defined in this factory."));

			aliases[alias] = functionName;
		}

		protected RoutineParameter Parameter(int i, TType type) {
			return new RoutineParameter(Alpha[i].ToString(CultureInfo.InvariantCulture), type);
		}

		protected RoutineParameter Dynamic(int i) {
			return Parameter(i, Function.DynamicType);
		}

		protected RoutineParameter Unbounded(int i, TType type) {
			return new RoutineParameter(Alpha[i].ToString(CultureInfo.InvariantCulture), type, ParameterAttributes.Unbounded);
		}

		protected RoutineParameter DynamicUnbounded(int i) {
			return Unbounded(i, Function.DynamicType);
		}

		protected void AddFunction(String name, Type type) {
			AddFunction(name, type, FunctionType.Static);
		}

		protected void AddFunction(String name, Type type, FunctionType functionType) {
			AddFunction(name, new RoutineParameter[0], type, functionType);
		}

		protected void AddFunction(String name, RoutineParameter[] parameters, Type type, FunctionType functionType) {
			try {
				if (IsFunctionDefined(name, parameters))
					throw new ApplicationException("Function '" + name + "' already defined in factory.");

				// We add these functions to the SYSTEM schema by default...
				var info = new FunctionInfo(new RoutineName(SystemSchema.Name, name), parameters) { FunctionType = functionType };
				functionTypeMapping[info] = type;
			} catch (Exception e) {
				throw new Exception(e.Message);
			}
		}

		protected void AddFunction(String name, RoutineParameter[] parameters, Type type) {
			AddFunction(name, parameters, type, FunctionType.Static);
		}

		protected void AddFunction(String name, RoutineParameter parameter, Type type) {
			AddFunction(name, parameter, type, FunctionType.Static);
		}

		protected void AddFunction(String name, RoutineParameter parameter, Type type, FunctionType functionType) {
			AddFunction(name, new[] { parameter }, type, functionType);
		}

		protected void Build(string name, RoutineParameter[] parameters, Func<RoutineInfo, IFunction> builder) {
			Build(name, parameters, builder, FunctionType.Static);
		}

		protected void Build(string name, Func<RoutineInfo, IFunction> builder) {
			Build(name, new RoutineParameter[0], builder);
		}

		protected void Build(string name, RoutineParameter parameter, Func<RoutineInfo, IFunction> builder) {
			Build(name, parameter, builder, FunctionType.Static);
		}

		protected void Build(string name, RoutineParameter parameter, Func<RoutineInfo, IFunction> builder, FunctionType functionType) {
			Build(name, new RoutineParameter[]{parameter}, builder, functionType);
		}

		protected void Build(string name, RoutineParameter[] parameters, Func<RoutineInfo, IFunction> builder, FunctionType functionType) {
			try {
				if (IsFunctionDefined(name, parameters))
					throw new ApplicationException("Function '" + name + "' already defined in factory.");

				// We add these functions to the SYSTEM schema by default...
				var info = new FunctionInfo(new RoutineName(SystemSchema.Name, name), parameters) { FunctionType = functionType };
				functionTypeMapping[info] = builder;
			} catch (Exception e) {
				throw new Exception(e.Message);
			}			
		}

		protected void Build(string name, RoutineParameter[] parameters, Action<FunctionBuilder> config) {
			Build(name, parameters, config, FunctionType.Static);
		}

		protected void Build(string name, Action<FunctionBuilder> config) {
			Build(name, new RoutineParameter[0], config);
		}

		protected void Build(string name, RoutineParameter parameter, Action<FunctionBuilder> config) {
			Build(name, parameter, config, FunctionType.Static);
		}

		protected void Build(string name, RoutineParameter parameter, Action<FunctionBuilder> config, FunctionType functionType) {
			Build(name, new RoutineParameter[] {parameter}, config, functionType);
		}

		protected void Build(string name, RoutineParameter[] parameters, Action<FunctionBuilder> config, FunctionType functionType) {
			try {
				if (IsFunctionDefined(name, parameters))
					throw new ApplicationException("Function '" + name + "' already defined in factory.");

				// We add these functions to the SYSTEM schema by default...
				var info = new FunctionInfo(new RoutineName(SystemSchema.Name, name), parameters) { FunctionType = functionType };
				functionTypeMapping[info] = config;
			} catch (Exception e) {
				throw new Exception(e.Message);
			}						
		}

		/// <summary>
		/// Removes a static function from this factory.
		/// </summary>
		/// <param name="name"></param>
		protected void RemoveFunction(String name) {
			var key = functionTypeMapping.Keys.FirstOrDefault(x => String.Equals(name, x.Name.Name, StringComparison.OrdinalIgnoreCase));
			if (key == null)
				throw new ApplicationException("Function '" + name + "' is not defined in this factory.");

			if (!functionTypeMapping.Remove(key))
				throw new ApplicationException("An error occurred while removing function '" + name + "' from the factory.");
		}

		/// <summary>
		/// Returns true if the function name is defined in this factory.
		/// </summary>
		/// <param name="name"></param>
		/// <returns></returns>
		protected bool IsFunctionDefined(String name) {
			return IsFunctionDefined(name, new RoutineParameter[0]);
		}

		protected bool IsFunctionDefined(string name, RoutineParameter[] parameters) {
			name = UnAlias(name);

			var info = functionTypeMapping.Where(x => x.Key.Name.Name.Equals(name, StringComparison.OrdinalIgnoreCase)).ToArray();
			if (info.Length == 0)
				return false;

			foreach (var mapping in info) {
				if (mapping.Key.Parameters.Length != parameters.Length)
					continue;

				if (mapping.Key.Parameters.Where((t, i) => t.Type.IsComparableType(parameters[i].Type)).Any())
					return true;
			}

			return false;
		}

		public void Init() {
			if (!initd) {
				OnInit();
				initd = true;
			}
		}

		protected abstract void OnInit();

		protected virtual IFunction OnFunctionCreate(RoutineInfo info, Type functionType) {
			const BindingFlags flags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
			var paramTypes = new Type[] {typeof (RoutineName), typeof (RoutineParameter[])};

			var ctor = functionType.GetConstructor(flags, null, paramTypes, null);
			if (ctor != null)
				return (IFunction) ctor.Invoke(null, new object[] {info.Name, info.Parameters});

			paramTypes = new Type[]{typeof(RoutineInfo)};
			ctor = functionType.GetConstructor(flags, null, paramTypes, null);
			if (ctor != null)
				return (IFunction) ctor.Invoke(null, new object[] {info});

			return null;
		}

		private string UnAlias(string name) {
			string alias;
			if (!aliases.TryGetValue(name, out alias))
				return name;

			return alias;
		}

		public IRoutine ResolveRoutine(RoutineInvoke invoke, IQueryContext context) {
			invoke.Name = UnAlias(invoke.Name);

			object arg = null;
			FunctionInfo info = null;
			foreach (var mapping in functionTypeMapping) {
				if (mapping.Key.MatchesInvoke(invoke, context)) {
					if (arg != null)
						throw new AmbiguousMatchException("More than one overload of '" + invoke.Name + "' matches the given call.");

					info = mapping.Key;
					arg = mapping.Value;
				}
			}

			if (arg == null)
				return null;

			if (arg is Type) {
				try {
					return OnFunctionCreate(info, (Type) arg);
				} catch (TargetInvocationException e) {
					throw new Exception(e.InnerException.Message);
				} catch (Exception e) {
					throw new Exception(e.Message);
				}
			}

			if (arg is Func<RoutineInfo, IFunction>) {
				return ((Func<RoutineInfo, IFunction>)arg)(info);
			}
			
			if (arg is Action<FunctionBuilder>) {
				var functionBuilder = FunctionBuilder.New(info.Name);
				functionBuilder = info.Parameters
					.Aggregate(functionBuilder,
						(current, param) => current.WithParameter(param.Name, param.Type, param.Direction, param.Attributes));

				functionBuilder = functionBuilder.OfType(info.FunctionType);

				((Action<FunctionBuilder>) arg)(functionBuilder);

				return functionBuilder;
			}

			return null;
		}

		public bool IsAggregateFunction(RoutineInvoke invoke, IQueryContext context) {
			var function = ResolveRoutine(invoke, context);
			if (function == null)
				return false;

			return ((IFunction) function).FunctionType == FunctionType.Aggregate;
		}
	}
}