using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Design.Conventions {
	public sealed class ConventionRegistry {
		private Dictionary<Type, ConventionInfo> conventions;

		internal ConventionRegistry()
			: this(new IConvention[0]) {
		}

		internal ConventionRegistry(IEnumerable<IConvention> initialConventions) {
			conventions = new Dictionary<Type, ConventionInfo>();

			if (initialConventions != null) {
				foreach (var convention in initialConventions) {
					Add(convention);
				}
			}
		}

		private int GreaterOffset() {
			return conventions.Values.Max(x => x.Offset);
		}

		private int OffsetOfType(Type conventionType) {
			return conventions.Values.Where(x => x.ConventionType == conventionType).Select(x => x.Offset).First();
		}

		private void ShiftFrom(int offset) {
			foreach (var convention in conventions.Values) {
				if (convention.Offset >= offset) {
					convention.Offset = convention.Offset++;
				}
			}
		}

		public void Add(params IConvention[] conventionsToAdd) {
			foreach (var convention in conventionsToAdd) {
				Add(convention);
			}
		}

		public void Add(IConvention convention) {
			if (convention == null)
				throw new ArgumentNullException("convention");

			var type = convention.GetType();
			if (conventions.ContainsKey(type))
				throw new ArgumentException(String.Format("It is possible to add one single convention type: another '{0}' is in the registry.", type));

			var offset = GreaterOffset();
			conventions[type] = new ConventionInfo(type, convention, offset + 1);
		}

		public void Add<TConvention>(TConvention convention) where TConvention : class, IConvention {
			Add((IConvention)convention);
		}

		public void Add<TConvention>() where TConvention : class, IConvention, new() {
			var convention = (TConvention) Activator.CreateInstance(typeof(TConvention));
			Add(convention);
		}

		public void AddAfter<TReference, TConvention>(TConvention convention)
			where TReference : class, IConvention
			where TConvention : class, IConvention {
			AddAfter(typeof(TReference), convention);
		}

		public void AddAfter(Type referenceType, IConvention convention) {
			if (referenceType == null)
				throw new ArgumentNullException("referenceType");
			if (!typeof(IConvention).IsAssignableFrom(referenceType))
				throw new ArgumentException(String.Format("Type '{0}' is not a convention.", referenceType));
			
			if (convention == null)
				throw new ArgumentNullException("convention");

			if (!conventions.ContainsKey(referenceType))
				throw new ArgumentException(String.Format("Could not find the reference convention of type '{0}' in the registry", referenceType));

			var type = convention.GetType();

			var offset = OffsetOfType(referenceType);
			ShiftFrom(offset+1);

			conventions[type] = new ConventionInfo(type, convention, offset + 1);
		}

		public void AddBefore<TReference, TConvention>(TConvention convention)
			where TReference : class, IConvention
			where TConvention : class, IConvention {
			AddBefore(typeof(TReference), convention);			
		}

		public void AddBefore(Type referenceType, IConvention convention) {
			if (referenceType == null)
				throw new ArgumentNullException("referenceType");
			if (!typeof(IConvention).IsAssignableFrom(referenceType))
				throw new ArgumentException(String.Format("Type '{0}' is not a convention.", referenceType));

			if (convention == null)
				throw new ArgumentNullException("convention");

			if (!conventions.ContainsKey(referenceType))
				throw new ArgumentException(String.Format("Could not find the reference convention of type '{0}' in the registry", referenceType));

			var type = convention.GetType();

			var offset = OffsetOfType(referenceType);
			ShiftFrom(offset - 1);

			conventions[type] = new ConventionInfo(type, convention, offset - 1);
		}

		public bool Remove<TConvention>() where TConvention : class, IConvention {
			return Remove(typeof(TConvention));
		}

		public bool Remove(Type conventionType) {
			if (conventionType == null)
				throw new ArgumentNullException("conventionType");
			if (!typeof(IConvention).IsAssignableFrom(conventionType))
				throw new ArgumentException(String.Format("Type '{0}' is not a convention.", conventionType));

			return conventions.Remove(conventionType);
		}

		internal IEnumerable<IConfigurationConvention> SortedConfigurationConventions() {
			return conventions.Values.Where(x => x.IsConfiguration)
				.OrderBy(x => x.Offset)
				.Select(x => x.Convention)
				.Cast<IConfigurationConvention>();
		}

		internal IEnumerable<IStructuralConvention> SortedStructuralConventions() {
			return conventions.Values.Where(x => x.IsStructural)
				.OrderBy(x => x.Offset)
				.Select(x => x.Convention)
				.Cast<IStructuralConvention>();
		}

		internal ConventionRegistry Clone() {
			return new ConventionRegistry {
				conventions = new Dictionary<Type, ConventionInfo>(conventions)
			};
		}

		#region ConventionInfo

		class ConventionInfo {
			public ConventionInfo(Type conventionType, IConvention convention, int offset) {
				ConventionType = conventionType;
				Convention = convention;
				Offset = offset;
			}

			public Type ConventionType { get; private set; }

			public IConvention Convention { get; private set; }

			public bool IsStructural {
				get { return Convention is IStructuralConvention; }
			}

			public bool IsConfiguration {
				get { return Convention is IConfigurationConvention; }
			}

			public int Offset { get; set; }
		}

		#endregion
	}
}
