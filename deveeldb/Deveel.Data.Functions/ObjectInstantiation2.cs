// 
//  ObjectInstantiation2.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Reflection;

namespace Deveel.Data.Functions {
	internal sealed class ObjectInstantiation2 : Function {
		public ObjectInstantiation2(Expression[] parameters)
			: base("_new_Object", parameters) {

			if (ParameterCount < 1) {
				throw new Exception("_new_Object function must have one argument.");
			}
		}

		public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
			// Resolve the parameters...
			int arg_len = ParameterCount - 1;
			TObject[] args = new TObject[arg_len];
			for (int i = 0; i < args.Length; ++i) {
				args[i] = this[i + 1].Evaluate(group, resolver, context);
			}
			Caster.DeserializeObjects(args);

			try {
				// Get the class name of the object to be constructed
				String clazz = this[0].Evaluate(null, resolver,
				                                context).Object.ToString();
				Type c = Type.GetType(clazz);
				ConstructorInfo[] constructs = c.GetConstructors();

				ConstructorInfo bestConstructor =
					Caster.FindBestConstructor(constructs, args);
				if (bestConstructor == null) {
					// Didn't find a match - build a list of class names of the
					// args so the user knows what we were looking for.
					String argTypes = Caster.GetArgTypesString(args);
					throw new Exception(
						"Unable to find a constructor for '" + clazz +
						"' that matches given arguments: " + argTypes);
				}
				Object[] casted_args =
					Caster.CastArgsToConstructor(args, bestConstructor);
				// Call the constructor to create the java object
				Object ob = bestConstructor.Invoke(casted_args);
				ByteLongObject serialized_ob = ObjectTranslator.Serialize(ob);
				return new TObject(new TObjectType(clazz), serialized_ob);

			} catch (TypeLoadException e) {
				throw new Exception("Class not found: " + e.Message);
			} catch (TypeInitializationException e) {
				throw new Exception("Instantiation ApplicationException: " + e.Message);
			} catch (AccessViolationException e) {
				throw new Exception("Illegal Access ApplicationException: " + e.Message);
			} catch (ArgumentException e) {
				throw new Exception("Illegal Argument ApplicationException: " + e.Message);
			} catch (TargetInvocationException e) {
				String msg = e.Message;
				if (msg == null) {
					Exception th = e.InnerException;
					if (th != null) {
						msg = th.GetType().Name + ": " + th.Message;
					}
				}
				throw new Exception("Invocation Target ApplicationException: " + msg);
			}

		}

		public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
			String clazz = this[0].Evaluate(null, resolver,
			                                context).Object.ToString();
			return new TObjectType(clazz);
		}

	}
}