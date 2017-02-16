package diva.genome.storage.hbase.allele.opencga;

import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantSqlQueryParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static diva.genome.storage.hbase.allele.count.AlleleCountToHBaseConverter.*;
import static diva.genome.storage.hbase.allele.count.HBaseAlleleCalculator.DEL_SYMBOL;
import static diva.genome.storage.hbase.allele.count.HBaseAlleleCalculator.INS_SYMBOL;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorUtils.isValidParam;
import static org.opencb.opencga.storage.hadoop.variant.index.VariantTableStudyRow.*;
import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.getStatsColumn;

/**
 * Created by mh719 on 15/02/2017.
 */
public class AlleleSqlQueryParser extends VariantSqlQueryParser {
    private static Logger logger = LoggerFactory.getLogger(AlleleSqlQueryParser.class);


    public static final String ONE_REF = Bytes.toString(buildQualifier(REFERENCE_PREFIX, 1));
    public static final String ONE_VAR = Bytes.toString(buildQualifier(VARIANT_PREFIX, 1));
    public static final String TWO_REF = Bytes.toString(buildQualifier(REFERENCE_PREFIX, 2));
    public static final String TWO_VAR = Bytes.toString(buildQualifier(VARIANT_PREFIX, 2));
    public static final String NO_CALL_REF = Bytes.toString(buildQualifier(REFERENCE_PREFIX, -1));
    public static final String ONE_DEL = Bytes.toString(buildQualifier(VARIANT_PREFIX, DEL_SYMBOL, 1));
    public static final String TWO_DEL = Bytes.toString(buildQualifier(VARIANT_PREFIX, DEL_SYMBOL, 2));
    public static final String ONE_INS = Bytes.toString(buildQualifier(VARIANT_PREFIX, INS_SYMBOL, 1));
    public static final String TWO_INS = Bytes.toString(buildQualifier(VARIANT_PREFIX, INS_SYMBOL, 2));
    public static final List<String> STUDY_COLUMNS = Collections.unmodifiableList(
            Arrays.asList(
                    NO_CALL_REF, // NO CALL
                    ONE_REF, // REF_HET
                    TWO_REF, //HOM_REF
                    ONE_VAR, // VAR HET
                    TWO_VAR, // VAR HOM
                    ONE_DEL, // DEL overlap
                    TWO_DEL,
                    ONE_INS, // INS overlap
                    TWO_INS,
                    FILTER_FAIL, FILTER_PASS
                    ));

    public AlleleSqlQueryParser(GenomeHelper genomeHelper, String variantTable, VariantDBAdaptorUtils utils, boolean
            clientSideSkip) {
        super(genomeHelper, variantTable, utils, clientSideSkip);
    }


    /**
     * Select only the required columns.
     *
     * Uses the params:
     * {@link VariantDBAdaptor.VariantQueryParams#RETURNED_STUDIES}
     * {@link VariantDBAdaptor.VariantQueryParams#RETURNED_SAMPLES}
     * {@link VariantDBAdaptor.VariantQueryParams#RETURNED_FILES}
     * {@link VariantDBAdaptor.VariantQueryParams#UNKNOWN_GENOTYPE}
     *
     * @param sb    SQLStringBuilder
     * @param query Query to parse
     * @param options   other options
     * @return String builder
     */
    @Override
    protected StringBuilder appendProjectedColumns(StringBuilder sb, Query query, QueryOptions options) {
        if (options.getBoolean(COUNT)) {
            return sb.append(" COUNT(*) ");
        } else {
            logger.debug("Add query to options: {} ", query.toJson());
            query.forEach((key, value) -> {
                options.put(key, value);
                logger.debug("Add {} with {} to options ... ", key, value);
            });
            options.putAll(query);
            Set<VariantField> returnedFields = VariantField.getReturnedFields(options);
            logger.debug("Returned Fields found: {} ", Arrays.toString(returnedFields.toArray()));

            List<Object> studyOptionList = options.getAsList(RETURNED_STUDIES.key());
            logger.debug("Found {} study id options ...", Arrays.toString(studyOptionList.toArray()));
            List<Integer> studyIds = getUtils().getStudyIds(studyOptionList, options);

            if (studyIds == null || studyIds.isEmpty()) {
                studyIds = getUtils().getStudyIds(options);
            }
            if (studyIds.size() > 1) {
                throw new IllegalStateException("Currently only one Study is supported, but found " + studyIds);
            }

            logger.debug("Returned studyIds found: {} ", Arrays.toString(studyIds.toArray()));

            sb.append(VariantPhoenixHelper.VariantColumn.CHROMOSOME).append(',')
                    .append(VariantPhoenixHelper.VariantColumn.POSITION).append(',')
                    .append(VariantPhoenixHelper.VariantColumn.REFERENCE).append(',')
                    .append(VariantPhoenixHelper.VariantColumn.ALTERNATE).append(',')
                    .append(VariantPhoenixHelper.VariantColumn.TYPE);

//            if (returnedFields.contains(VariantField.STUDIES)) {
                for (Integer studyId : studyIds) {
                    List<String> studyColumns = STUDY_COLUMNS;
                    for (String studyColumn : studyColumns) {
                        sb.append(",\"").append(buildColumnKey(studyId, studyColumn)).append('"');
                    }
                    if (returnedFields.contains(VariantField.STUDIES_STATS)) {
                        StudyConfiguration studyConfiguration = getUtils().getStudyConfigurationManager()
                                .getStudyConfiguration(studyId, null).first();
                        for (Integer cohortId : studyConfiguration.getCohortIds().values()) {
//                        for (Integer cohortId : studyConfiguration.getCalculatedStats()) {
                            PhoenixHelper.Column statsColumn = getStatsColumn(studyId, cohortId);
                            sb.append(",\"").append(statsColumn.column()).append('"');
                        }
                    }
//                }
            }

            if (returnedFields.contains(VariantField.ANNOTATION)) {
                sb.append(',').append(VariantPhoenixHelper.VariantColumn.FULL_ANNOTATION);
            }

            return sb;
        }
    }

    private String buildColumnKey(Integer studyId, String studyColumn) {
        return studyColumn;
    }

    protected StudyConfiguration addVariantFilters(Query query, QueryOptions options, List<String> filters) {
        addQueryFilter(query, REFERENCE, VariantPhoenixHelper.VariantColumn.REFERENCE, filters);

        addQueryFilter(query, ALTERNATE, VariantPhoenixHelper.VariantColumn.ALTERNATE, filters);

        addQueryFilter(query, TYPE, VariantPhoenixHelper.VariantColumn.TYPE, filters, s -> {
            VariantType type = VariantType.valueOf(s);
            Set<VariantType> subTypes = Variant.subTypes(type);
            ArrayList<VariantType> types = new ArrayList<>(subTypes.size() + 1);
            types.add(type);
            types.addAll(subTypes);
            return types;
        });

        final StudyConfiguration defaultStudyConfiguration;
//        if (isValidParam(query, STUDIES)) { // TODO
//            String value = query.getString(STUDIES.key());
//            VariantDBAdaptorUtils.QueryOperation operation = checkOperator(value);
//            List<String> values = splitValue(value, operation);
//            StringBuilder sb = new StringBuilder();
//            Iterator<String> iterator = values.iterator();
//            Map<String, Integer> studies = getUtils().getStudyConfigurationManager().getStudies(options);
//            Set<Integer> notNullStudies = new HashSet<>();
//            while (iterator.hasNext()) {
//                String study = iterator.next();
//                Integer studyId = getUtils().getStudyId(study, false, studies);
//                if (study.startsWith("!")) {
//                    sb.append("\"").append(buildColumnKey(studyId, VariantTableStudyRow.HOM_REF)).append("\" IS NULL ");
//                } else {
//                    notNullStudies.add(studyId);
//                    sb.append("\"").append(buildColumnKey(studyId, VariantTableStudyRow.HOM_REF)).append("\" IS NOT NULL ");
//                }
//                if (iterator.hasNext()) {
//                    if (operation == null || operation.equals(VariantDBAdaptorUtils.QueryOperation.AND)) {
//                        sb.append(" AND ");
//                    } else {
//                        sb.append(" OR ");
//                    }
//                }
//            }
//            // Skip this filter if contains all the existing studies.
//            if (studies.values().size() != notNullStudies.size() || !notNullStudies.containsAll(studies.values())) {
//                filters.add(sb.toString());
//            }
//            List<Integer> studyIds = getUtils().getStudyIds(values, options);
//            if (studyIds.size() == 1) {
//                defaultStudyConfiguration = getUtils().getStudyConfigurationManager().getStudyConfiguration(studyIds.get(0), options).first();
//            } else {
//                defaultStudyConfiguration = null;
//            }
//        } else {
            List<Integer> studyIds = getUtils().getStudyConfigurationManager().getStudyIds(options);
            if (studyIds.size() == 1) {
                defaultStudyConfiguration = getUtils().getStudyConfigurationManager().getStudyConfiguration(studyIds.get(0), options).first();
            } else {
                defaultStudyConfiguration = null;
            }
//        }

        unsupportedFilter(query, FILES);

        if (isValidParam(query, COHORTS)) {
            for (String cohort : query.getAsStringList(COHORTS.key())) {
                boolean negated = false;
                if (cohort.startsWith("!")) {
                    cohort = cohort.substring(1);
                    negated = true;
                }
                String[] studyCohort = cohort.split(":");
                StudyConfiguration studyConfiguration;
                if (studyCohort.length == 2) {
                    studyConfiguration = getUtils().getStudyConfiguration(studyCohort[0], defaultStudyConfiguration);
                    cohort = studyCohort[1];
                } else if (studyCohort.length == 1) {
                    studyConfiguration = defaultStudyConfiguration;
                } else {
                    throw VariantQueryException.malformedParam(COHORTS, query.getString((COHORTS.key())), "Expected {study}:{cohort}");
                }
                int cohortId = getUtils().getCohortId(cohort, studyConfiguration);
                PhoenixHelper.Column column = VariantPhoenixHelper.getStatsColumn(studyConfiguration.getStudyId(), cohortId);
                if (negated) {
                    filters.add(column + " IS NULL");
                } else {
                    filters.add(column + " IS NOT NULL");
                }
            }
        }

        //
        //
        // NA12877_01 :  0/0  ;  NA12878_01 :  0/1  ,  1/1
        if (isValidParam(query, GENOTYPE)) {
            for (String sampleGenotype : query.getAsStringList(GENOTYPE.key(), ";")) {
                //[<study>:]<sample>:<genotype>[,<genotype>]*
                String[] split = sampleGenotype.split(":");
                final List<String> genotypes;
                int studyId;
                int sampleId;
                if (split.length == 2) {
                    if (defaultStudyConfiguration == null) {
                        List<String> studyNames = getUtils().getStudyConfigurationManager().getStudyNames(null);
                        throw VariantQueryException.missingStudyForSample(split[0], studyNames);
                    }
                    studyId = defaultStudyConfiguration.getStudyId();
                    sampleId = getUtils().getSampleId(split[0], defaultStudyConfiguration);
                    genotypes = Arrays.asList(split[1].split(","));
                } else if (split.length == 3) {
                    studyId = getUtils().getStudyId(split[0], null, false);
                    sampleId = getUtils().getSampleId(split[1], defaultStudyConfiguration);
                    genotypes = Arrays.asList(split[2].split(","));
                } else {
                    throw VariantQueryException.malformedParam(GENOTYPE, sampleGenotype);
                }

                List<String> gts = new ArrayList<>(genotypes.size());
                for (String genotype : genotypes) {
                    boolean negated = false;
                    if (genotype.startsWith("!")) {
                        genotype = genotype.substring(1);
                        negated = true;
                    }
                    switch (genotype) {
                        case HET_REF:
                            gts.add((negated ? " NOT (" : " (") + sampleId + " = ANY(\"" + buildColumnKey(studyId, ONE_VAR) + "\") "
                                    + " AND " + sampleId + " = ANY(\"" + buildColumnKey(studyId, ONE_REF) + "\")  )");
                            break;
                        case HOM_VAR:
                            gts.add((negated ? " NOT " : " ") + sampleId + " = ANY(\"" + buildColumnKey(studyId, TWO_VAR) + "\") ");
                            break;
                        case NOCALL:
//                        0 = any("1_.")
                            gts.add((negated ? " NOT " : " ") + sampleId + " = ANY(\"" + buildColumnKey(studyId, NO_CALL_REF) + "\") ");
                            break;
                        case HOM_REF:
                            List<String> subFilters = new ArrayList<>(4);
                            String pref = negated ? " OR " : " AND NOT ";
                            String q = (negated ? "( " : " NOT ")
                                + sampleId + " = ANY(\"" + buildColumnKey(studyId, ONE_VAR) + "\") "
                                + pref + sampleId + " = ANY(\"" + buildColumnKey(studyId, ONE_REF) + "\") "
                                + pref + sampleId + " = ANY(\"" + buildColumnKey(studyId, TWO_VAR) + "\") "
                                + pref + sampleId + " = ANY(\"" + buildColumnKey(studyId, NO_CALL_REF) + "\") "
                                + pref + sampleId + " = ANY(\"" + buildColumnKey(studyId, ONE_DEL) + "\") "
                                + pref + sampleId + " = ANY(\"" + buildColumnKey(studyId, TWO_DEL) + "\") "
                                + pref + sampleId + " = ANY(\"" + buildColumnKey(studyId, ONE_INS) + "\") "
                                + pref + sampleId + " = ANY(\"" + buildColumnKey(studyId, TWO_INS) + "\") "
                                + (negated ? " ) " : "");
                            gts.add(q);
                            break;
                        default:  //OTHER
                            gts.add((negated ? " NOT (" : " (") + sampleId + " = ANY(\"" + buildColumnKey(studyId, ONE_DEL) + "\") "
                                    + " OR " + sampleId + " = ANY(\"" + buildColumnKey(studyId, TWO_DEL) + "\") "
                                    + " OR " + sampleId + " = ANY(\"" + buildColumnKey(studyId, ONE_INS) + "\") "
                                    + " OR " + sampleId + " = ANY(\"" + buildColumnKey(studyId, TWO_INS) + "\") "
                                    + " )");
                            break;
                    }
                }
                filters.add(gts.stream().collect(Collectors.joining(" OR ", " ( ", " ) ")));
            }
        }

        return defaultStudyConfiguration;
    }
}
