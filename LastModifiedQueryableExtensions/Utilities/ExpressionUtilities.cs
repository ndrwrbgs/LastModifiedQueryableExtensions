namespace LastModifiedQueryableExtensions.Utilities
{
    using System;
    using System.Linq.Expressions;

    internal static class ExpressionFactory
    {
        public static Expression<Func<T, bool>> CreateIsAfterFilter<T>(
            Expression<Func<T, DateTimeOffset>> getTimestampFromItem,
            DateTimeOffset timestampExclusive)
        {
            // Translate Expression<Func<T, DateTimeOffset>> to Expression<Func<T, bool>>
            // aka Translate 'get date for item' to 'is newer than last cache date'
            var inputParameter = Expression.Parameter(typeof(T));
            var invocation = Expression.Invoke(getTimestampFromItem, inputParameter);
            var dateValue = Expression.Constant(timestampExclusive);
            var isAfterDate = Expression.GreaterThan(invocation, dateValue);
            Expression<Func<T, bool>> isAfterDateLambda = Expression.Lambda<Func<T, bool>>(isAfterDate, inputParameter);
            return isAfterDateLambda;
        }
    }
}