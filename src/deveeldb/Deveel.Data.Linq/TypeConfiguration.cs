using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Deveel.Data.Linq {
	public class TypeConfiguration<T> : ITypeConfiguration where T : class {
		private KeyConfiguration keyConfiguration;
		private Dictionary<string, MemberConfiguration> members;

		public TypeConfiguration() {
			members = new Dictionary<string, MemberConfiguration>();
		}

		public KeyConfiguration HasKey(Expression<Func<T, object>> selector) {
			var memberInfo = TypeUtil.SelectMember(selector);
			keyConfiguration = new KeyConfiguration(memberInfo);
			return keyConfiguration;
		}

		public MemberConfiguration Member(Expression<Func<T, object>> selector) {
			var memberInfo = TypeUtil.SelectMember(selector);

			MemberConfiguration configuration;
			if (!members.TryGetValue(memberInfo.Name, out configuration)) {
				configuration = new MemberConfiguration(memberInfo);
				members[memberInfo.Name] = configuration;
			}

			return configuration;
		}

		DbTypeModel ITypeConfiguration.CreateModel() {
			throw new NotImplementedException();
		}
	}
}
