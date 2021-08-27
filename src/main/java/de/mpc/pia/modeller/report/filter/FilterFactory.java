package de.mpc.pia.modeller.report.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import de.mpc.pia.modeller.report.filter.impl.PSMScoreFilter;
import de.mpc.pia.modeller.report.filter.impl.PSMTopIdentificationFilter;
import de.mpc.pia.modeller.report.filter.impl.PeptideScoreFilter;



/**
 * This class contains some helper methods to instantiate
 * {@link AbstractFilter}s and their subclasses for filtering the reports.
 *
 * @author julian
 *
 */
public class FilterFactory {

    /**
     * We don't ever want to instantiate this class
     */
    private FilterFactory() {
        throw new AssertionError();
    }


    /**
     * gets the available comparators for the filter given by the short name
     *
     * @param filterShort
     * @return
     */
    public static List<FilterComparator> getAvailableComparators(String filterShort) {
        if (filterShort != null) {
            FilterType type = getFilterType(filterShort);
            if (type != null) {
                return type.getAvailableComparators();
            }
        }

        return new ArrayList<>();
    }


    /**
     * Builds a new instance of the filter type, given by the short name.
     * The comparatorName must be a {@link FilterComparator}'s name valid for
     * this filter type and the input of a valid type.
     *
     * @param filterShort
     * @param comparatorName
     * @param input
     * @param negate
     * @param messageBuffer
     * @return
     */
    public static AbstractFilter newInstanceOf(String filterShort,
            String comparatorName, String input, boolean negate,
            StringBuilder messageBuffer) {

        FilterComparator comparator = FilterComparator.getFilterComparatorByName(comparatorName);
        if (comparator == null) {
            // no comparator selected
            messageBuffer.append("Please select a comparator!");
            return null;
        }

        return newInstanceOf(filterShort, comparator, input, negate,
                messageBuffer);
    }


    /**
     * Builds a new instance of the filter type, given the comparator.
     * The comparatorName must be a {@link FilterComparator}'s name valid for
     * this filter type and the input of a valid type.
     *
     * @param filterShort
     * @param input
     * @param negate
     * @param messageBuffer
     * @return
     */
    public static AbstractFilter newInstanceOf(String filterShort,
            FilterComparator comparator, String input, boolean negate,
            StringBuilder messageBuffer) {

        String filterString = filterShort;
        if (filterString == null) {
            filterString = "null";
        }

        if (comparator == null) {
            // no comparator selected
            messageBuffer.append("Please select a comparator!");
            return null;
        }

        Object value = parseValueForFilterType(input, filterString, messageBuffer);
        AbstractFilter filter = null;

        if (value != null) {
            if (value instanceof Number) {
                filter = newInstanceOfScoreboundFilter(filterString, comparator, negate, (Number)value);
            }

            if (filter == null) {
                RegisteredFilters regFilter = RegisteredFilters.getFilterByShortname(filterString);
                if ((regFilter != null) && regFilter.isCorrectValueInstance(value)) {
                    filter = regFilter.newInstanceOf(comparator, value, negate);
                }
            }
        }

        return filter;
    }


    /**
     * Parses the given {@link String} to the object and value according to the
     * {@link FilterType} specified by the filterShort String.
     *
     * @param input
     * @param filterShort
     * @param messageBuffer any error or warning is added to this
     * @return
     */
    private static Object parseValueForFilterType(String input, String filterShort,
            StringBuilder messageBuffer) {
        FilterType type = getFilterType(filterShort);
        Object value = null;

        if (type == null) {
            messageBuffer.append("invalid filter (null)");
        } else {
            try {
                switch (type) {
                case numerical:
                    value = Double.parseDouble(input);
                    break;

                case bool:
                    value = Boolean.parseBoolean(input);
                    break;

                case literal:
                case literal_list:
                case modification:
                    value = input;
                    break;

                default:
                    // no valid filter selected
                    messageBuffer.append("Please select a filter!");
                    value = null;
                }
            } catch (NumberFormatException e) {
                messageBuffer.append("Please enter a numerical value!");
                value = null;
            }
        }

        return value;
    }


    /**
     * Creates a new instance of a special {@link AbstractFilter}, i.e. a filter
     * that corresponds to a peptide or PSM score. If no such score is defined
     * by filterShort, null is returned.
     *
     * @param filterShort
     * @param comparator
     * @param negate
     * @param value
     * @return
     */
    private static AbstractFilter newInstanceOfScoreboundFilter(String filterShort,
            FilterComparator comparator, boolean negate, Number value) {
        AbstractFilter filter = null;

        if (filterShort.startsWith(PSMScoreFilter.PREFIX)) {
            filter = new PSMScoreFilter(comparator, negate, value.doubleValue(),
                    filterShort.substring(PSMScoreFilter.PREFIX.length()));
        } else if (filterShort.startsWith(PeptideScoreFilter.PREFIX)) {
            filter = new PeptideScoreFilter(comparator, negate, value.doubleValue(),
                    filterShort.substring(PeptideScoreFilter.PREFIX.length()));
        } else if (filterShort.startsWith(PSMTopIdentificationFilter.PREFIX)) {
            filter = new PSMTopIdentificationFilter(comparator, value.intValue(), negate,
                    filterShort.substring(PSMTopIdentificationFilter.PREFIX.length()));
        }

        return filter;
    }


    /**
     * Returns the FilterType of the given filter.
     *
     * @param filterShort
     * @return
     */
    public static FilterType getFilterType(String filterShort) {
        if (filterShort == null) {
            return null;
        }

        FilterType type = null;

        if (filterShort.startsWith(PSMScoreFilter.PREFIX)) {
            type = PSMScoreFilter.filterType;
        } else if (filterShort.startsWith(PeptideScoreFilter.PREFIX)) {
            type = PeptideScoreFilter.filterType;
        } else if (filterShort.startsWith(PSMTopIdentificationFilter.PREFIX)) {
            type = PSMTopIdentificationFilter.filterType;
        } else {
            RegisteredFilters filter = RegisteredFilters.getFilterByShortname(filterShort);
            if (filter != null) {
                type = filter.getFilterType();
            }
        }

        return type;
    }


    /**
     * Applies the filtering given by the filters to the given List of
     * Filterables and returns the filtered list.
     *
     * @param <T>
     * @param reportItems
     * @param filters
     * @param fileID
     * @return
     */
    public static <T extends Filterable> List<T> applyFilters(
            List<T> reportItems, List<AbstractFilter> filters, Long fileID) {
        if ((filters == null) || filters.isEmpty()) {
            return reportItems;
        }

        List<T> filteredReportItems = new ArrayList<>();

        if (reportItems != null) {
            filteredReportItems.addAll(reportItems.stream().filter(item -> satisfiesFilterList(item, fileID, filters)).collect(Collectors.toList()));
        }

        return filteredReportItems;
    }


    /**
     * Applies the filtering given by the filters to the given List of
     * Filterables and returns the filtered list. Uses 0L as fileID.
     *
     * @param <T>
     * @param reportItems
     * @param filters
     * @return
     */
    public static <T extends Filterable> List<T> applyFilters(List<T> reportItems,
            List<AbstractFilter> filters) {
        return applyFilters(reportItems, filters, 0L);
    }


    /**
     * Checks whether all the inference filters in the given List are satisfied
     * for the filterable object.
     *
     * @return
     */
    public static <T extends Filterable> boolean satisfiesFilterList(T item,
            Long fileID, List<AbstractFilter> filters) {
        if ((filters == null) || filters.isEmpty()) {
            return true;
        }

        boolean satisfiesAllFilters = true;

        for (AbstractFilter filter : filters) {
            if (filter.supportsClass(item)) {
                satisfiesAllFilters &= filter.satisfiesFilter(item, fileID);
            }
        }

        return satisfiesAllFilters;
    }
    
    
    /**
     * Creates a filter instance from the given string representation (e.g. from 
     * the analysis JSON)
     * 
     * @param filterStr
     * @return
     */
	public static AbstractFilter createInstanceFromString(String filterStr, StringBuilder messageBuffer) {
		AbstractFilter filter = null;
		String[] splits = filterStr.split("\\s+", 3);
		
		if (splits.length > 2) {
			boolean negate = false;
			
			if (splits[1].startsWith("!") || "not".equals(splits[1])) {
				negate = true;
				splits[1] = splits[1].substring(1);
			}
			if ("not".equals(splits[1])) {
				splits[1] = splits[2];
				splits[2] = (splits.length > 3) ? splits[3] : null;
			}
			
			filter = FilterFactory.newInstanceOf(
					splits[0],
	                FilterComparator.getFilterComparatorByString(splits[1]),
	                splits[2],
	                negate,
	                messageBuffer);
		} else {
			messageBuffer.append("Too few parameters in filter definition '" + filterStr + "'");
		}
		
		return filter; 
	}

}
