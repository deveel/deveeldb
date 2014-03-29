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
using System.Collections;
using System.Reflection;

using Deveel.Data.Types;

namespace Deveel.Data.Mapping {
	public sealed class TypeMapping : ICloneable {
		public TypeMapping(Type type, UserTypeAttributes attributes)
			: this(type, type.Name, attributes) {
		}

		public TypeMapping(Type type, string name, UserTypeAttributes attributes) {
			if (type == null)
				throw new ArgumentNullException("type");
			if (type.IsInterface)
				throw new ArgumentException("Interface mappings are not supported.");

			this.name = name;
			this.type = type;
			this.attributes = attributes;
		}

		private readonly Type type;
		private readonly string name;
		private readonly UserTypeAttributes attributes;
		private ArrayList memberMappings;

		public Type Type {
			get { return type; }
		}

		public UserTypeAttributes Attributes {
			get { return attributes; }
		}

		public string Name {
			get { return name; }
		}

		private static bool FilterMember(MemberInfo memberInfo, object criteria) {
			if (Attribute.IsDefined(memberInfo, typeof(IgnoreAttribute)))
				return false;

			if (memberInfo is FieldInfo)
				return true;

			// we can only use readable and writeable properties...
			PropertyInfo propertyInfo = (PropertyInfo)memberInfo;
			return propertyInfo.CanRead && propertyInfo.CanWrite;
		}

		internal static TypeMapping FromType(Type type) {
			UserTypeAttributes attributes = new UserTypeAttributes();
			if (type.IsSealed)
				attributes |= UserTypeAttributes.Sealed;
			else if (type.IsAbstract)
				attributes |= UserTypeAttributes.Abstract;

			TypeMapping typeMapping = new TypeMapping(type, attributes);

			MemberInfo[] memberInfos =
				type.FindMembers(global::System.Reflection.MemberTypes.Field | global::System.Reflection.MemberTypes.Property,
				                 BindingFlags.Instance | BindingFlags.Public, new MemberFilter(FilterMember), null);

			for (int i = 0; i < memberInfos.Length; i++) {
				TypeAttributeMapping mapping = TypeAttributeMapping.FromMember(typeMapping, memberInfos[i]);

				if (typeMapping.memberMappings == null)
					typeMapping.memberMappings = new ArrayList();

				typeMapping.memberMappings.Add(mapping);
			}

			return typeMapping;
		}

		public bool HasMappedAttribute(string name) {
			return FindMember(name, MemberTypes.Attribute) != null;
		}

		public TypeAttributeMapping AddAttribute(string name) {
			if (name == null)
				throw new ArgumentNullException("name");

			if (HasMappedAttribute(name))
				throw new ArgumentException("The member '" + name + "' was already mapped.");

			TypeAttributeMapping mapping = new TypeAttributeMapping(this, name, null, true);

			if (memberMappings == null)
				memberMappings = new ArrayList();

			memberMappings.Add(mapping);

			return mapping;
		}

		public TypeAttributeMapping GetAttribute(string name) {
			if (name == null)
				throw new ArgumentNullException("name");

			TypeMemberMapping mapping = FindMember(name, MemberTypes.Attribute);
			if (mapping == null)
				return null;

			TypeAttributeMapping attributeMapping = mapping as TypeAttributeMapping;
			if (attributeMapping == null)
				throw new ArgumentException("The member '" + name + "' is not an attribute.");

			return attributeMapping;
		}

		public TypeMemberMapping FindMember(string name, MemberTypes types) {
			if (memberMappings == null)
				return null;

			int sz = memberMappings.Count;
			if (sz == 0)
				return null;

			for (int i = 0; i < sz; i++) {
				TypeMemberMapping memberMapping = memberMappings[i] as TypeMemberMapping;
				if (memberMapping == null)
					continue;

				if ((memberMapping.MemberType & types) != 0 &&
					memberMapping.Name == name)
					return memberMapping;
			}

			return null;
		}

		#region Implementation of ICloneable

		public object Clone() {
			TypeMapping mapping = new TypeMapping(type, (string)name.Clone(), attributes);
			if (memberMappings != null)
				mapping.memberMappings = (ArrayList) memberMappings.Clone();
			return mapping;
		}

		#endregion
	}
}