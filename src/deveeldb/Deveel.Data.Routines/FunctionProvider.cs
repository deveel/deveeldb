// 
//  Copyright 2010-2016 Deveel
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
using System.Linq.Expressions;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

using DryIoc;

namespace Deveel.Data.Routines {
	public abstract class FunctionProvider : IRoutineResolver, IDisposable {
		private Container container;

		protected FunctionProvider() {
			container = new Container(Rules.Default.WithResolveIEnumerableAsLazyEnumerable());
			CallInit();
		}

		~FunctionProvider() {
			Dispose(false);
		}

		private void CallInit() {
			OnInit();
		}

		protected abstract void OnInit();

		protected void Register(SystemFunctionInfo functionInfo, Func<InvokeContext, InvokeResult> body, Func<InvokeContext, SqlType> returnType) {
			Register(functionInfo, body, null, returnType);
		}

		protected void Register(SystemFunctionInfo functionInfo, Func<InvokeContext, InvokeResult> body, Func<InvokeContext, Field, Field> afterAggregate, Func<InvokeContext, SqlType> returnType) {
			if (afterAggregate != null &&
				functionInfo.FunctionType != FunctionType.Aggregate)
				throw new ArgumentException("Cannot specify an after-aggregation on non-aggregate function.");

			Register(new DelegateFunction(functionInfo, body, afterAggregate, returnType));
		}

		protected void Register(Func<IFunctionConfiguration, IFunctionConfiguration> config) {
			if (config == null)
				return;

			var configuration = new FunctionConfiguration(this);
			config(configuration);

			var functionInfos = configuration.FunctionInfo;
			foreach (var functionInfo in functionInfos) {
				Register(functionInfo, configuration.ExecuteFunc, configuration.AfterAggregate, configuration.ReturnTypeFunc);
			}
		}

		protected void Register(IFunction function) {
			if (function == null)
				throw new ArgumentNullException("function");

			var functionName = function.ObjectInfo.FullName.Name.ToUpperInvariant();
			container.RegisterInstance(function, serviceKey: functionName,reuse:Reuse.Singleton);
		}

		IRoutine IRoutineResolver.ResolveRoutine(Invoke invoke, IRequest request) {
			return ResolveFunction(invoke, request);
		}

		public IFunction ResolveFunction(Invoke invoke, IRequest request) {
			var name = invoke.RoutineName.Name;

			if (name == null)
				return null;

			var functionName = name.ToUpperInvariant();
			var functions = container.ResolveMany<IFunction>(serviceKey:functionName).ToArrayOrSelf();
			if (functions.Length == 0)
				return null;
			if (functions.Length == 1)
				return functions[0];

			return functions.FirstOrDefault(x => x.RoutineInfo.MatchesInvoke(invoke, request));
		}

		#region DelegateFunction

		class DelegateFunction : Function {
			private readonly Func<InvokeContext, InvokeResult> functionBody;
			private readonly Func<InvokeContext, SqlType> returnType; 
			private readonly Func<InvokeContext, Field, Field> afterAggregate ;

			public DelegateFunction(FunctionInfo functionInfo, Func<InvokeContext, InvokeResult> functionBody,
				Func<InvokeContext, Field, Field> afterAggregate, Func<InvokeContext, SqlType> returnType)
				: base(functionInfo) {
				this.functionBody = functionBody;
				this.afterAggregate = afterAggregate;
				this.returnType = returnType;
			}

			public override InvokeResult Execute(InvokeContext context) {
				if (FunctionInfo.FunctionType != FunctionType.Aggregate)
					return functionBody(context);

				if (context.GroupResolver == null)
					throw new Exception(String.Format("Function '{0}' can only be used as an aggregate.", FunctionInfo.RoutineName));

				Field result = null;

				// All aggregates functions return 'null' if group size is 0
				int size = context.GroupResolver.Count;
				if (size == 0) {
					// Return a NULL of the return type
					return context.Result(Field.Null(ReturnType(context)));
				}

				Field val;
				SqlReferenceExpression v = context.Arguments[0] as SqlReferenceExpression;

				// If the aggregate parameter is a simple variable, then use optimal
				// routine,
				if (v != null) {
					for (int i = 0; i < size; ++i) {
						var variable = context.GroupResolver.Resolve(v.ReferenceName, i);
						val = variable.GetValue(context.Request);

						var invokeResult = functionBody(context.New(new SqlExpression[] {
							SqlExpression.Constant(result),
							SqlExpression.Constant(val)
						}));

						result = invokeResult.ReturnValue;
					}
				} else {
					// Otherwise we must resolve the expression for each entry in group,
					// This allows for expressions such as 'sum(quantity * price)' to
					// work for a group.
					var exp = context.Arguments[0];
					for (int i = 0; i < size; ++i) {
						var evaluated = exp.Evaluate(context.Request, context.GroupResolver.GetVariableResolver(i));

						if (evaluated.ExpressionType != SqlExpressionType.Constant)
							throw new InvalidOperationException(
								String.Format("The evaluation of the group {0} in aggregate function '{1}' is not constant", i,
									FunctionInfo.RoutineName));

						val = ((SqlConstantExpression) evaluated).Value;

						var invokeResult = functionBody(context.New(new SqlExpression[] {
							SqlExpression.Constant(result),
							SqlExpression.Constant(val)
						}));

						result = invokeResult.ReturnValue;
					}
				}

				// Post method.
				if (afterAggregate!= null)
					result = afterAggregate(context, result);

				return context.Result(result);
			}

			public override SqlType ReturnType(InvokeContext context) {
				if (returnType == null)
					return FunctionInfo.ReturnType;

				return returnType(context);
			}
		}

		#endregion

		#region FunctionConfiguration

		class FunctionConfiguration : IAggregateFunctionConfiguration {
			private readonly FunctionProvider provider;
			private readonly Dictionary<string, RoutineParameter> parameters;
			private List<string> aliases;

			public FunctionConfiguration(FunctionProvider provider) {
				this.provider = provider;
				parameters = new Dictionary<string, RoutineParameter>();
				FunctionType = FunctionType.Static;
			}

			public FunctionType FunctionType { get; private set; }

			public Func<InvokeContext, SqlType> ReturnTypeFunc { get; private set; }

			public Func<InvokeContext, InvokeResult> ExecuteFunc { get; private set; }

			public Func<InvokeContext, Field, Field> AfterAggregate { get; private set; } 

			public SystemFunctionInfo[] FunctionInfo {
				get {
					var result = new List<SystemFunctionInfo> { new SystemFunctionInfo(FunctionName, parameters.Values.ToArray(), FunctionType) };
					if (aliases != null && aliases.Count > 0)
						result.AddRange(aliases.Select(name => new SystemFunctionInfo(name, parameters.Values.ToArray(), FunctionType)));

					return result.ToArray();
				}
			}

			public string FunctionName { get; private set; }

			public RoutineParameter[] Parameters {
				get { return parameters.Values.ToArray(); }
			}

			public bool HasParameter(string name) {
				return parameters.ContainsKey(name);
			}

			public bool HasUnboundedParameter() {
				return parameters.Values.Any(x => x.IsUnbounded);
			}

			public IFunctionConfiguration Named(string name) {
				if (name == null)
					throw new ArgumentNullException("name");

				FunctionName = name;
				return this;
			}

			public IFunctionConfiguration OfType(FunctionType functionType) {
				FunctionType = functionType;
				return this;
			}

			public IFunctionConfiguration WithAlias(string alias) {
				if (alias == null)
					throw new ArgumentNullException("alias");

				if (FunctionName == null)
					throw new ArgumentException("The function has no name configured and cannot be aliased.");

				if (aliases == null)
					aliases = new List<string>();

				aliases.Add(alias);

				return this;
			}

			public IFunctionConfiguration WithParameter(Action<IFunctionParameterConfiguration> config) {
				var paramConfig = new FunctionParameterConfiguration(this);
				if (config != null) {
					config(paramConfig);

					var param = paramConfig.AsParameter();

					if (String.IsNullOrEmpty(param.Name))
						throw new InvalidOperationException("A parameter must define a name.");

					parameters.Add(param.Name, param);
				}

				return this;
			}

			public IFunctionConfiguration ReturnsType(Func<InvokeContext, SqlType> returns) {
				ReturnTypeFunc = returns;
				return this;
			}

			public IFunctionConfiguration WhenExecute(Func<InvokeContext, InvokeResult> execute) {
				ExecuteFunc = execute;
				return this;
			}

			public IAggregateFunctionConfiguration OnAfterAggregate(Func<InvokeContext, Field, Field> afterAggregate) {
				AfterAggregate = afterAggregate;
				return this;
			}
		}

		#endregion

		#region FunctionParemeterConfiguration

		class FunctionParameterConfiguration : IFunctionParameterConfiguration {
			private readonly FunctionConfiguration configuration;

			private string parameterName;
			private SqlType sqlType;
			private ParameterAttributes attributes;

			public FunctionParameterConfiguration(FunctionConfiguration configuration) {
				this.configuration = configuration;

				attributes = new ParameterAttributes();
				sqlType = PrimitiveTypes.Numeric();
			}

			public IFunctionParameterConfiguration Named(string name) {
				if (String.IsNullOrEmpty(name))
					throw new ArgumentNullException("name");

				if (configuration.HasParameter(name))
					throw new ArgumentException(String.Format("A parameter with name '{0}' was already configured for the function", name), "name");

				parameterName = name;

				return this;
			}

			public IFunctionParameterConfiguration OfType(SqlType type) {
				if (type == null)
					throw new ArgumentNullException("type");

				sqlType = type;

				return this;
			}

			public IFunctionParameterConfiguration Unbounded(bool flag) {
				if (configuration.HasUnboundedParameter())
					throw new ArgumentException("An unbounded parameter is already configured");

				if (flag)
					attributes |= ParameterAttributes.Unbounded;

				return this;
			}

			public RoutineParameter AsParameter() {
				return new RoutineParameter(parameterName, sqlType, attributes);
			}
		}

		#endregion

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing) {
			if (disposing) {
				if (container != null)
					container.Dispose();
			}

			container = null;
		}
	}
}
