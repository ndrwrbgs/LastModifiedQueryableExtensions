namespace LastModifiedQueryableExtensions.Utilities
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;

    internal static class ExpressionFactory
    {
        public static Expression<Func<T, bool>> CreateIsAfterFilter<T>(
            Expression<Func<T, DateTimeOffset>> getTimestampFromItem,
            DateTimeOffset timestampExclusive)
        {
            // Translate Expression<Func<T, DateTime>> to Expression<Func<T, bool>>
            // aka Translate 'get date for item' to 'is newer than last cache date'
            var inputParameter = Expression.Parameter(typeof(T), "a");
            
            //Expression getTimestampExpression;
            //if (getTimestampFromItem.Body is MemberExpression)
            //{
            //    //getTimestampExpression = Expression.Lambda<Func<DateTimeOffset>>(getTimestampFromItem, inputParameter);
            //    //getTimestampExpression = Expression.Invoke(getTimestampFromItem, inputParameter);
            //    getTimestampExpression = Expression.Lambda(getTimestampFromItem.Body, inputParameter);
            //}
            //else
            //{
            //    getTimestampExpression = Expression.Invoke(getTimestampFromItem, inputParameter);
            //}

            ////var nullValue = Expression.Constant(null, typeof(DateTime?));
            ////var isNotNull = Expression.NotEqual(getTimestampExpression, nullValue);

            //var dateValue = Expression.Constant(timestampExclusive, typeof(DateTimeOffset));
            //var isAfterDate = Expression.GreaterThan(getTimestampExpression, dateValue);

            ////var and = Expression.And(isNotNull, isAfterDate);

            //Expression<Func<T, bool>> isAfterDateLambda = Expression.Lambda<Func<T, bool>>(isAfterDate, inputParameter);
            
            var isAfterDateLambda = Expression.Lambda<Func<T, bool>>(
                Expression.GreaterThan(
                    ParameterRebinder.ReplaceParameters(
                        getTimestampFromItem.Parameters.ToDictionary(p => p, p => inputParameter),
                        getTimestampFromItem.Body),
                    Expression.Constant(timestampExclusive, typeof(DateTimeOffset))),
                inputParameter);
            ;

            Expression isAfterFilter = isAfterDateLambda.Body;
            return isAfterDateLambda;
        }

        public class ParameterRebinder : ExpressionVisitor {


            private readonly Dictionary<ParameterExpression, ParameterExpression> map;


 


            public ParameterRebinder(Dictionary<ParameterExpression, ParameterExpression> map) {


                this.map = map ?? new Dictionary<ParameterExpression, ParameterExpression>();


            }


 


            public static Expression ReplaceParameters(Dictionary<ParameterExpression, ParameterExpression> map, Expression exp) {


                return new ParameterRebinder(map).Visit(exp);


            }


 


            protected override Expression VisitParameter(ParameterExpression p) {


                ParameterExpression replacement;


                if (map.TryGetValue(p, out replacement)) {


                    p = replacement;


                }


                return base.VisitParameter(p);


            }


        }
    }
}