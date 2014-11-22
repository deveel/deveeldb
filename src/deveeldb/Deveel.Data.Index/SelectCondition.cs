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

namespace Deveel.Data.Index {
	/// <summary>
	/// A conditional operator used to compute binary operations
	/// of comparison on a set.
	/// </summary>
	public struct SelectCondition {
		private SelectCondition(ConditionType conditionType, bool isAll, bool isAny)
			: this() {
			ConditionType = conditionType;
			IsAll = isAll;
			IsAny = isAny;
		}

		/// <summary>
		/// Constructs a plain conditional operator with the given type.
		/// </summary>
		/// <param name="conditionType">The type of binary operator.</param>
		public SelectCondition(ConditionType conditionType)
			: this(conditionType, false, false) {
		}

		/// <summary>
		/// Gets the type of conditional operator.
		/// </summary>
		public ConditionType ConditionType { get; private set; }

		/// <summary>
		/// Gets a boolean value indicating if this is a special <c>ALL</c> condition.
		/// </summary>
		public bool IsAll { get; private set; }

		/// <summary>
		/// Gets a boolean value indicating if this is a special <c>ANY</c> condition.
		/// </summary>
		public bool IsAny { get; private set; }

		/// <summary>
		/// Gets a boolean value indicating if this is either a <c>ANY</c> or <c>ALL</c>
		/// subquery condition.
		/// </summary>
		public bool IsSubQuery {
			get { return IsAll || IsAny; }
		}

		/// <summary>
		/// Creates a new <c>ALL</c> binary operator.
		/// </summary>
		/// <param name="operatorType">The type of binary operator to compose
		/// with the <c>ALL</c> operator.</param>
		/// <returns>
		/// Returns an instance of <see cref="SelectCondition"/> that represents
		/// an <c>ALL</c> operator.
		/// </returns>
		public static SelectCondition All(ConditionType operatorType) {
			return new SelectCondition(operatorType, true, false);
		}

		/// <summary>
		/// Creates a new <c>ANY</c> binary operator.
		/// </summary>
		/// <param name="operatorType">The type of binary operator to compose
		/// with the <c>ANY</c> operator.</param>
		/// <returns>
		/// Returns an instance of <see cref="SelectCondition"/> that represents
		/// an <c>ANY</c> operator.
		/// </returns>
		public static SelectCondition Any(ConditionType operatorType) {
			return new SelectCondition(operatorType, false, true);
		}

		/// <summary>
		/// Gets the plain version of this select condition .
		/// </summary>
		/// <remarks>
		/// <para>
		/// If this condition is either <c>ALL</c> or <c>ANY</c>
		/// the returned version will not be neither of that form.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns an instance of <see cref="SelectCondition"/> that is
		/// a plain version of this condition.
		/// </returns>
		public SelectCondition Plain() {
			return new SelectCondition(ConditionType);
		}

		/// <summary>
		/// Gets an inversed form of this condition.
		/// </summary>
		/// <returns>
		/// Returns an instance of <see cref="SelectCondition"/> that is the
		/// inverse form of this condition.
		/// </returns>
		public SelectCondition Inverse() {
			if (IsSubQuery) {
				bool isAny = false, isAll = false;
				if (IsAny) {
					isAll = true;
				} else if (IsAll) {
					isAny = true;
				} else {
					throw new Exception("Can not handle sub-query form.");
				}

				var plainInverse = Plain().Inverse().ConditionType;
				return new SelectCondition(plainInverse, isAll, isAny);
			}

			if (ConditionType == ConditionType.Equal)
				return new SelectCondition(ConditionType.NotEqual);
			if (ConditionType == ConditionType.NotEqual)
				return new SelectCondition(ConditionType.Equal);
			if (ConditionType == ConditionType.GreaterThan)
				return new SelectCondition(ConditionType.SmallerOrEqualThan);
			if (ConditionType == ConditionType.SmallerThan)
				return new SelectCondition(ConditionType.GreaterOrEqualThan);
			if (ConditionType == ConditionType.GreaterOrEqualThan)
				return new SelectCondition(ConditionType.SmallerThan);
			if (ConditionType == ConditionType.SmallerOrEqualThan)
				return new SelectCondition(ConditionType.GreaterThan);

			//TODO: AND and OR ???

			if (ConditionType == ConditionType.Like)
				return new SelectCondition(ConditionType.NotLike);
			if (ConditionType == ConditionType.NotLike)
				return new SelectCondition(ConditionType.Like);
			if (ConditionType == ConditionType.Is)
				return new SelectCondition(ConditionType.IsNot);
			if (ConditionType == ConditionType.IsNot)
				return new SelectCondition(ConditionType.Is);

			throw new ApplicationException("Cannot inverse this condition.");
		}

		/// <summary>
		/// Creates a reversed version of this condition.
		/// </summary>
		/// <returns>
		/// Returns an instance of <see cref="SelectCondition"/> that is the
		/// reversed version of this operator.
		/// </returns>
		/// <exception cref="ApplicationException">
		/// In case this operator is not conditional, hence it's not reversable.
		/// </exception>
		public SelectCondition Reverse() {
			if (ConditionType == ConditionType.Equal ||
			    ConditionType == ConditionType.NotEqual ||
			    ConditionType == ConditionType.Is ||
			    ConditionType == ConditionType.IsNot)
				return this;
			if (ConditionType == ConditionType.GreaterThan)
				return new SelectCondition(ConditionType.SmallerThan);
			if (ConditionType == ConditionType.SmallerThan)
				return new SelectCondition(ConditionType.GreaterThan);
			if (ConditionType == ConditionType.GreaterOrEqualThan)
				return new SelectCondition(ConditionType.SmallerOrEqualThan);
			if (ConditionType == ConditionType.SmallerOrEqualThan)
				return new SelectCondition(ConditionType.GreaterOrEqualThan);

			throw new ApplicationException("Can't reverse a non conditional operator.");
		}

		/// <inheritdoc/>
		public override bool Equals(object obj) {
			var other = (SelectCondition) obj;
			return ConditionType == other.ConditionType &&
			       IsAll == other.IsAll &&
			       IsAny == other.IsAny;
		}

		public override int GetHashCode() {
			var sub = 1;
			if (IsAll)
				sub = 2;
			else if (IsAny)
				sub = 4;

			return ConditionType.GetHashCode() ^ sub;
		}

		public override string ToString() {
			return base.ToString();
		}
	}
}