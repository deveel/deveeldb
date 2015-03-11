// 
//  Copyright 2010-2015 Deveel
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
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

using Deveel.Data.Configuration;
using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Routines {
	public abstract class FunctionFactory : IRoutineResolver {
		private readonly Dictionary<FunctionInfo, object> functionTypeMapping;
		private readonly Dictionary<string, string> aliases;
 
		private bool initd;

		protected FunctionFactory() {
			functionTypeMapping = new Dictionary<FunctionInfo, object>();
			aliases = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);

			GlobExpression = SqlExpression.Constant("*");
			GlobList = new SqlExpression[] { GlobExpression };
		}

		private static readonly char[] Alpha = "abcdefghkjilmnopqrstuvwxyz".ToCharArray();

		public static SqlExpression GlobExpression { get; private set; }

		public static SqlExpression[] GlobList { get; private set; }

		protected void Alias(string functionName, string alias) {
			if (!functionTypeMapping.Any(x => String.Equals(x.Key.Name.Name, functionName, StringComparison.OrdinalIgnoreCase)))
				throw new ArgumentException(String.Format("Function {0} was not defined by this factory.", functionName));

			if (functionTypeMapping.Any(x => String.Equals(x.Key.Name.Name, alias, StringComparison.OrdinalIgnoreCase)))
				throw new InvalidOperationException(String.Format("A function named '{0}' is already defined in this factory."));

			aliases[alias] = functionName;
		}

		protected void AddFunction(IFunction function) {
			if (function == null)
				throw new ArgumentNullException("function");

			try {
				if (IsFunctionDefined(function.Name.ToString(), function.Parameters))
					throw new DatabaseConfigurationException(String.Format("Function '{0}' is already defined in factory.",
						function.Name));

				var info = new FunctionInfo(function.Name, function.Parameters) {FunctionType = function.FunctionType};
				functionTypeMapping[info] = function;
			} catch (DatabaseConfigurationException) {
				throw;
			} catch (Exception ex) {
				throw new DatabaseConfigurationException("Unable to add the function to the factory", ex);
			}
		}

		protected void AddFunction(FunctionInfo info, Type type) {
			if (info == null)
				throw new ArgumentNullException("info");
			if (type == null)
				throw new ArgumentNullException("type");

			try {
				if (IsFunctionDefined(info.Name.ToString(), info.Parameters))
					throw new DatabaseConfigurationException(String.Format("Function '{0}' is already defined in factory.", info));
				if (!typeof(IFunction).IsAssignableFrom(type))
					throw new ArgumentException(String.Format("The type '{0}' is not a valid function.", type));

				functionTypeMapping[info] = type;
			} catch(DatabaseConfigurationException) {
				throw;
			} catch (Exception ex) {
				throw new DatabaseConfigurationException("An error occurred while adding a function to the facory", ex);
			}
		}

		protected void AddFunction(string name, Type type) {
			AddFunction(name, type, FunctionType.Static);
		}

		protected void AddFunction(string name, Type type, FunctionType functionType) {
			AddFunction(name, new RoutineParameter[0], type, functionType);
		}

		protected void AddFunction(string name, RoutineParameter[] parameters, Type type, FunctionType functionType) {
				// We add these functions to the SYSTEM schema by default...
			AddFunction(new FunctionInfo(new ObjectName(SystemSchema.SchemaName, name), parameters) { FunctionType = functionType }, type);
		}

		protected void AddFunction(string name, RoutineParameter[] parameters, Type type) {
			AddFunction(name, parameters, type, FunctionType.Static);
		}

		protected void AddFunction(string name, RoutineParameter parameter, Type type) {
			AddFunction(name, parameter, type, FunctionType.Static);
		}

		protected void AddFunction(string name, RoutineParameter parameter, Type type, FunctionType functionType) {
			AddFunction(name, new[] { parameter }, type, functionType);
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

				if (mapping.Key.Parameters.Where((t, i) => t.Type.IsComparable(parameters[i].Type)).Any())
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
			var paramTypes = new Type[] {typeof (ObjectName), typeof (RoutineParameter[])};

			var ctor = functionType.GetConstructor(flags, null, paramTypes, null);
			if (ctor != null)
				return (IFunction) ctor.Invoke(null, new object[] {info.Name, info.Parameters});

			paramTypes = new Type[]{typeof(RoutineInfo)};
			ctor = functionType.GetConstructor(flags, null, paramTypes, null);
			if (ctor != null)
				return (IFunction) ctor.Invoke(null, new object[] {info});

			return null;
		}

		private RoutineInvoke UnAlias(RoutineInvoke invoke) {
			var name = invoke.RoutineName.Name;

			string alias;
			if (!aliases.TryGetValue(name, out alias))
				return invoke;

			return new RoutineInvoke(new ObjectName(invoke.RoutineName.Parent, alias), invoke.Arguments);
		}

		private string UnAlias(string name) {
			string alias;
			if (!aliases.TryGetValue(name, out alias))
				return name;

			return alias;
		}

		public IRoutine ResolveRoutine(RoutineInvoke invoke, IQueryContext context) {
			invoke = UnAlias(invoke);

			object arg = null;
			FunctionInfo info = null;
			foreach (var mapping in functionTypeMapping) {
				if (mapping.Key.MatchesInvoke(invoke, context)) {
					if (arg != null)
						throw new AmbiguousMatchException(String.Format("More than one overload of '{0}' matches the given call.", invoke.RoutineName));

					info = mapping.Key;
					arg = mapping.Value;
				}
			}

			if (arg == null)
				return null;

			if (arg is IFunction)
				return (IFunction) arg;

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