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
using System.Reflection;

namespace Deveel.Data.Build {
	public class FeatureBuilder {
		private BuiltFeature feature;

		public FeatureBuilder() {
			feature = new BuiltFeature();
		}

		public FeatureBuilder Named(string value) {
			if (String.IsNullOrEmpty(value))
				throw new ArgumentNullException("value");

			feature.Name = value;
			return this;
		}

		public FeatureBuilder Version(string value) {
			feature.Version = value;
			return this;
		}

		public FeatureBuilder WithAssemblyVersion() {
			var assembly = Assembly.GetCallingAssembly();
			var assemblyName = assembly.GetName();
			return Version(assemblyName.Version.ToString(3));
		}

		public FeatureBuilder WithAssemblyVersion(Type type) {
			var assembly = Assembly.GetAssembly(type);
			var assemblyName = assembly.GetName();
			return Version(assemblyName.Version.ToString(3));
		}

		public FeatureBuilder OnSystemBuild(Action<ISystemBuilder> build) {
			feature.BuildHandler = build;
			return this;
		}

		public FeatureBuilder OnSystemEvent(SystemEventType eventType, Action<IQuery> handler) {
			Action<IQuery> oldHandler;
			if (feature.HandledEvents.TryGetValue(eventType, out oldHandler)) {
				handler = (Action<IQuery>) Delegate.Combine(oldHandler, handler);
			}

			feature.HandledEvents[eventType] = handler;
			return this;
		}

		public FeatureBuilder OnDatabaseCreate(Action<IQuery> handler) {
			return OnSystemEvent(SystemEventType.DatabaseCreate, handler);
		}

		public FeatureBuilder OnTableCompositeSetup(Action<IQuery> handler) {
			return OnSystemEvent(SystemEventType.TableCompositeSetup, handler);
		}

		public FeatureBuilder OnTableCompositeCreate(Action<IQuery> handler) {
			return OnSystemEvent(SystemEventType.TableCompositeCreate, handler);
		}

		public FeatureBuilder From<TFeature>() where TFeature : class, new() {
			return From(typeof(TFeature));
		}

		public FeatureBuilder From(Type featureType) {
			if (featureType == null)
				throw new ArgumentNullException("featureType");
			if (!featureType.IsClass)
				throw new ArgumentException(String.Format("The type '{0}' is not a class.", featureType));

			var obj = Activator.CreateInstance(featureType);
			return From(obj);
		}

		public FeatureBuilder From(object obj) {
			if (obj == null)
				throw new ArgumentNullException("obj");

			if (obj is ISystemFeature) {
				var from = (ISystemFeature) obj;
				feature.Name = from.Name;
				feature.Version = from.Version;
				feature.BuildHandler = from.OnBuild;
				feature.SystemEventHandler = from.OnSystemEvent;
			} else {
				var type = obj.GetType();

				var attribute = Attribute.GetCustomAttribute(type, typeof(FeatureAttribute))
					as FeatureAttribute;

				if (attribute != null) {
					feature.Name = attribute.Name;
					feature.Version = attribute.Version;
				}

				DiscoverName(type, obj);
				DiscoverVersion(type, obj);
				DiscoverOnBuild(type, obj);
			}

			return this;
		}

		private static string FindMemberValue(Type type, object obj, params string[] names) {
			var flags = BindingFlags.Public | BindingFlags.NonPublic;
			var memberTypes = MemberTypes.Field | MemberTypes.Property;

			foreach (var name in names) {
				var members = type.GetMember(name, memberTypes, flags);

				if (members.Length == 1) {
					var member = members[0];
					if (member is PropertyInfo) {
						var prop = (PropertyInfo) member;

						if (prop.PropertyType != typeof(string))
							throw new InvalidOperationException(String.Format("The property '{0}' of type '{1}' is not a string.",
								prop.Name, type));

						var method = prop.GetGetMethod(true);
						if (method.IsStatic) {
							return (string) method.Invoke(null, null);
						} else {
							return (string) method.Invoke(obj, null);
						}
					} else {
						var field = (FieldInfo) member;

						if (field.FieldType != typeof(string))
							throw new InvalidOperationException(String.Format("The field '{0}' of type '{1}' is not a string.",
								field.Name, type));

						if (field.IsStatic)
							return (string) field.GetValue(null);

						return (string) field.GetValue(obj);
					}
				}
			}

			return null;
		}

		private void DiscoverName(Type type, object obj) {
			var value = FindMemberValue(type, obj, "Name", "FeatureName");
			if (value != null)
				feature.Name = value;
		}

		private void DiscoverVersion(Type type, object obj) {
			var value = FindMemberValue(type, obj, "Version", "FeatureVersion");
			if (value != null)
				feature.Version = value;
		}

		private static Action<TArg> FindMethod<TArg>(Type type, object obj, params string[] names) {
			var flags = BindingFlags.Public | BindingFlags.NonPublic;
			var argTypes = new Type[] {typeof(TArg)};

			foreach (var name in names) {
				var method = type.GetMethod(name, flags, null, argTypes, null);
				if (method != null) {
					return arg => {
						if (method.IsStatic) {
							method.Invoke(null, new object[] {arg});
						} else {
							method.Invoke(obj, new object[] {arg});
						}
					};
				}
			}

			return null;
		}

		private void DiscoverOnBuild(Type type, object obj) {
			var handler = FindMethod<ISystemBuilder>(type, obj, "OnBuild", "Build");
			if (handler != null)
				feature.BuildHandler = handler;
		}

		private void DiscoverOnSystemEvent(Type type, object obj) {
			var handler = FindMethod<SystemEvent>(type, obj, "OnSystemEvent", "SystemEvent");
			if (handler != null)
				feature.SystemEventHandler = handler;
		}

		public ISystemFeature Build() {
			if (String.IsNullOrEmpty(feature.Name))
				throw new InvalidOperationException("The name of the feature is required.");
			if (String.IsNullOrEmpty(feature.Version))
				throw new InvalidOperationException("The version of the feature was not specified");

			return feature;
		}

		#region BuiltFeature

		class BuiltFeature : ISystemFeature {
			public BuiltFeature() {
				HandledEvents = new Dictionary<SystemEventType, Action<IQuery>>();
			}

			public string Name { get; set; }

			public string Version { get; set; }

			public IDictionary<SystemEventType, Action<IQuery>> HandledEvents { get; private set; }

			public Action<SystemEvent> SystemEventHandler { get; set; }

			public Action<ISystemBuilder> BuildHandler { get; set; }

			public void OnBuild(ISystemBuilder builder) {
				if (BuildHandler != null)
					BuildHandler(builder);
			}

			public void OnSystemEvent(SystemEvent @event) {
				if (SystemEventHandler != null) {
					SystemEventHandler(@event);
				} else {
					Action<IQuery> handler;
					if (!HandledEvents.TryGetValue(@event.EventType, out handler))
						return;

					handler(@event.SystemQuery);
				}
			}
		}

		#endregion
	}
}
