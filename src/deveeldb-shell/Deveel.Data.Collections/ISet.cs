using System;
using System.Collections;

namespace Deveel.Collections {
	public interface ISet : ICollection {
		bool IsEmpty { get; }


		void Add(object obj);

		void AddRange(ICollection c);

		void Remove(object obj);

		void Clear();

		bool Contains(object obj);

		bool ContainsRange(ICollection c);

		bool Equals(object obj);

		int GetHashCode();

		object[] ToArray();

		Array ToArray(Type type);
	}
}