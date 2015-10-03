using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace Deveel.Data.Mapping {
	public class TypeMappingConfiguration<T> : ITypeMappingConfiguration where T : class {
		private Dictionary<string, IMemberMappingConfiguration> memberMappings;
		private List<string> ignoredMembers;

		public TypeMappingConfiguration() {
			TableName = DiscoverTableName();
			memberMappings = DiscoverMembers();
		}

		private string TableName { get; set; }

		private string UniqueKeyMember { get; set; }

		string ITypeMappingConfiguration.TableName {
			get { return TableName; }
		}

		string ITypeMappingConfiguration.UniqueKeyMember {
			get { return UniqueKeyMember; }
		}

		IEnumerable<KeyValuePair<string, IMemberMappingConfiguration>> ITypeMappingConfiguration.Members {
			get { return memberMappings; }
		}

		private Dictionary<string, IMemberMappingConfiguration> DiscoverMembers() {
			var dictionary = new Dictionary<string, IMemberMappingConfiguration>();

			var members = typeof (T).GetMembers(BindingFlags.Instance | BindingFlags.Public);
			foreach (var memberInfo in members) {
				if (!Attribute.IsDefined(memberInfo, typeof (IgnoreAttribute), false)) {
					dictionary[memberInfo.Name] = new MemberMappingConfiguration<T>(this, memberInfo);
				} else {
					Ignore(memberInfo.Name);
				}
			}

			return dictionary;
		}

		private string DiscoverTableName() {
			var tableNameAttr = Attribute.GetCustomAttribute(typeof(T), typeof(TableAttribute), false)
				as TableAttribute;
			if (tableNameAttr == null)
				return null;
			return tableNameAttr.TableName;
		}

		private MemberInfo GetMemberInfo<TProperty>(Expression<Func<T, TProperty>> memberSelector) {
			var type = typeof(T);

			var member = memberSelector.Body as MemberExpression;
			if (member == null)
				throw new ArgumentException(string.Format("Expression '{0}' refers to a method, not a property.", memberSelector));

			var memberInfo = member.Member;
			if (memberInfo == null)
				throw new ArgumentException(string.Format("Expression '{0}' not refers to a field or a property.", memberSelector));

			if (type != memberInfo.ReflectedType &&
				!type.IsSubclassOf(memberInfo.ReflectedType))
				throw new ArgumentException(string.Format("Expresion '{0}' refers to a member that is not from type {1}.", memberSelector, type));

			return memberInfo;
		}

		public TypeMappingConfiguration<T> ToTable(string name) {
			TableName = name;
			return this;
		}  

		public TypeMappingConfiguration<T> HasUniqueKey<TMember>(Expression<Func<T, TMember>> member) {
			if (!String.IsNullOrEmpty(UniqueKeyMember))
				throw new InvalidOperationException("A unique key member is already configured.");

			var memberInfo = GetMemberInfo(member);
			UniqueKeyMember = memberInfo.Name;
			return this;
		}

		public MemberMappingConfiguration<T> Member<TMember>(Expression<Func<T, TMember>> member) {
			var memberInfo = GetMemberInfo(member);
			var memberName = memberInfo.Name;

			if (ignoredMembers != null &&
			    ignoredMembers.Contains(memberName))
				throw new InvalidOperationException(String.Format("Member '{0}' is ignored and cannot be configured.", memberName));

			IMemberMappingConfiguration configuration;
			if (!memberMappings.TryGetValue(memberName, out configuration)) {
				configuration = new MemberMappingConfiguration<T>(this, memberInfo);
				memberMappings[memberName] = configuration;
			}

			return (MemberMappingConfiguration<T>) configuration;
		}

		public void Ignore<TMember>(Expression<Func<T, TMember>> member) {
			var memberInfo = GetMemberInfo(member);
			var memberName = memberInfo.Name;

			Ignore(memberName);
		}

		internal void Ignore(string memberName) {
			if (ignoredMembers == null)
				ignoredMembers = new List<string>();

			if (!ignoredMembers.Contains(memberName))
				ignoredMembers.Add(memberName);
		}
	}
}
