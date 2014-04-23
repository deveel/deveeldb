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
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;

using Deveel.Data.DbSystem;
using Deveel.Data.Types;

namespace Deveel.Data.Routines {
	/// <summary>
	/// A factory that generates <see cref="IFunction"/> objects given 
	/// a function name and a set of expression's that represent parameters.
	/// </summary>
	/// <remarks>
	/// A developer may create their own instance of this class and register 
	/// the factory with the <see cref="DatabaseContext"/>. When the SQL grammer 
	/// comes across a function, it will try and resolve the function name against 
	/// the registered function factories.
	/// </remarks>
	public abstract class LegacyFunctionFactory : IFunctionLookup {
		private bool initd;
		private static readonly Expression GlobExpression;

		static LegacyFunctionFactory() {
			GlobExpression = new Expression();
			GlobExpression.AddElement(TObject.CreateString("*"));
			GlobExpression.Text.Append("*");

			GlobList = new Expression[] { GlobExpression };
		}

		///<summary>
		/// Represents a function argument * for glob's such as <c>count(*)</c>
		///</summary>
		public static readonly Expression[] GlobList;

		/// <summary>
		/// The mapping of 'name' to 'functionType' for each function that's 
		/// registered with this factory.
		/// </summary>
		private readonly IDictionary<FunctionInfo, Type> functionTypeMapping;

		/// <summary>
		/// Constructor arguments types for the function.
		/// </summary>
		private readonly Type[] constructProto;

		protected LegacyFunctionFactory() {
			functionTypeMapping = new Dictionary<FunctionInfo, Type>();

			// The is the prototype for the constructor when creating a new function.
			constructProto = new Type[1];
			Object expArrOb = new Expression[0];
			constructProto[0] = expArrOb.GetType();
		}

		private static readonly char[] Alpha = "abcdefghkjilmnopqrstuvwxyz".ToCharArray();

		protected RoutineParameter Parameter(int i, TType type) {
			return new RoutineParameter(Alpha[i].ToString(CultureInfo.InvariantCulture), type);
		}

		protected RoutineParameter Dynamic(int i) {
			return Parameter(i, InvokedFunction.DynamicType);
		}

		protected RoutineParameter Unbounded(int i, TType type) {
			return new RoutineParameter(Alpha[i].ToString(CultureInfo.InvariantCulture), type, ParameterAttributes.Unbounded);
		}

		protected RoutineParameter DynamicUnbounded(int i) {
			return Unbounded(i, InvokedFunction.DynamicType);
		}

		/// <summary>
		/// Adds a new function to this factory.
		/// </summary>
		/// <param name="name">The name of the function (eg. 'sum', 'concat').</param>
		/// <param name="type">The <see cref="IFunction"/> type that we instantiate 
		/// for this function.</param>
		/// <remarks>
		/// Takes a function name and a class that is the <see cref="IFunction"/>
		/// implementation. When the <see cref="GenerateFunction"/> method is called, 
		/// it looks up the class with the function name and returns a new instance 
		/// of the function.
		/// </remarks>
		protected void AddFunction(String name, Type type) {
			AddFunction(name, type, FunctionType.Static);
		}

		/// <summary>
		/// Adds a new function to this factory.
		/// </summary>
		/// <param name="name">The name of the function (eg. 'sum', 'concat').</param>
		/// <param name="type">The <see cref="IFunction"/> type that we instantiate 
		/// for this function.</param>
		/// <param name="functionType">That type of function.</param>
		/// <remarks>
		/// Takes a function name and a class that is the <see cref="IFunction"/>
		/// implementation. When the <see cref="GenerateFunction"/> method is called, 
		/// it looks up the class with the function name and returns a new instance 
		/// of the function.
		/// </remarks>
		protected void AddFunction(String name, Type type, FunctionType functionType) {
			AddFunction(name, new RoutineParameter[0], type, functionType);
		}

		/// <summary>
		/// Adds a new function to this factory.
		/// </summary>
		/// <param name="name">The name of the function (eg. 'sum', 'concat').</param>
		/// <param name="parameters"></param>
		/// <param name="type">The <see cref="IFunction"/> type that we instantiate 
		/// for this function.</param>
		/// <param name="functionType">That type of function.</param>
		/// <remarks>
		/// Takes a function name and a class that is the <see cref="IFunction"/>
		/// implementation. When the <see cref="GenerateFunction"/> method is called, 
		/// it looks up the class with the function name and returns a new instance 
		/// of the function.
		/// </remarks>
		protected void AddFunction(String name, RoutineParameter[] parameters, Type type, FunctionType functionType) {
			try {
				if (IsFunctionDefined(name))
					throw new ApplicationException("Function '" + name + "' already defined in factory.");

				// We add these functions to the SYSTEM schema by default...
				var info = new FunctionInfo(new RoutineName(SystemSchema.Name, name), parameters) { FunctionType = functionType};
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
			AddFunction(name, new[]{parameter}, type, functionType);
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
			return functionTypeMapping.Keys.Any(info => String.Equals(info.Name.Name, name, StringComparison.OrdinalIgnoreCase));
		}

		/// <summary>
		/// Initializes this <see cref="LegacyFunctionFactory"/>.
		/// </summary>
		/// <remarks>
		/// This is an abstract method that needs to be implemented. (It doesn't 
		/// need to do anything if a developer implements their own version 
		/// of <see cref="GenerateFunction"/>).
		/// </remarks>
		public void Init() {
			if (!initd) {
				OnInit();
				initd = true;
			}
		}

		protected abstract void OnInit();

		public IFunction ResolveRoutine(RoutineInvoke invoke, IQueryContext context) {
			Type functionType = null;
			foreach (var mapping in functionTypeMapping) {
				if (mapping.Key.MatchesInvoke(invoke, context)) {
					if (functionType != null)
						throw new AmbiguousMatchException();

					functionType = mapping.Value;
				}
			}

			if (functionType == null)
				return null;

			object[] args = new Object[] { invoke.Arguments };
			try {
				return (IFunction) Activator.CreateInstance(functionType,
					BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
					null,
					args,
					CultureInfo.InvariantCulture,
					null);
			} catch (TargetInvocationException e) {
				throw new Exception(e.InnerException.Message);
			} catch (Exception e) {
				throw new Exception(e.Message);
			}
		}

		/// <summary>
		/// Creates a <see cref="IFunction"/> object for the function 
		/// with the given name with the given arguments.
		/// </summary>
		/// <param name="routineInvoke"></param>
		/// <remarks>
		/// If this factory does not handle a function with the given name then it returns null.
		/// </remarks>
		/// <returns></returns>
		public IFunction GenerateFunction(RoutineInvoke routineInvoke) {
			return (IFunction) ResolveRoutine(routineInvoke, null);
		}

		/// <summary>
		/// Checks if the given function is aggregate.
		/// </summary>
		/// <param name="routineInvoke"></param>
		/// <returns>
		/// Returns true if the function defined by <see cref="RoutineInvoke"/> is 
		/// an aggregate function, or false otherwise.
		/// </returns>
		public bool IsAggregate(RoutineInvoke routineInvoke) {
			FunctionInfo f_info = GetFunctionInfo(routineInvoke.Name);
			if (f_info == null)
				// Function not handled by this factory so return false.
				return false;

			return (f_info.FunctionType == FunctionType.Aggregate);
		}

		///<summary>
		/// Returns a <see cref="FunctionInfo"/> instance of the function with the given 
		/// name that this <see cref="LegacyFunctionFactory"/> manages.
		///</summary>
		///<param name="name"></param>
		/// <remarks>
		/// If <see cref="GenerateFunction"/> is reimplemented then this method should be 
		/// rewritten also.
		/// </remarks>
		///<returns></returns>
		public FunctionInfo GetFunctionInfo(String name) {
			return (functionTypeMapping.Where(mapping => mapping.Key.Name.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
				.Select(mapping => mapping.Key))
				.FirstOrDefault();
		}

		///<summary>
		/// Returns the list of all function names that this FunctionFactory manages.
		///</summary>
		/// <remarks>
		/// This is used to compile information about the function factories.  If
		/// <see cref="GenerateFunction"/> is reimplemented then this method should 
		/// be rewritten also.
		/// </remarks>
		///<returns></returns>
		public FunctionInfo[] GetAllFunctionInfo() {
			return functionTypeMapping.Keys.ToArray();
		}
	}
}