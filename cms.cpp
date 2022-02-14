#include <cmath>
#include <limits>
#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <fstream>
#include <iostream>
#include <random>
#include <thread>
#include <stdexcept>
#define timer timer_for_boost_progress_t
#include <boost/progress.hpp>
#undef timer
#include <boost/timer/timer.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/variance.hpp>
#include <glog/logging.h>
#define FMT_HEADER_ONLY 1
#include <fmt/printf.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/numpy.h>
#include <pybind11/eigen.h>
#include <readerwritercircularbuffer.h>
#include <bitsery/bitsery.h>
#include <bitsery/adapter/stream.h>
#include <bitsery/traits/vector.h>
#include <unsupported/Eigen/CXX11/Tensor>

namespace cms {

    using std::numeric_limits;
    using std::string;
    using std::vector;
    using std::pair;
    using std::map;
    using std::unordered_map;
    using std::unordered_set;
    using std::ifstream;
    namespace py = pybind11;

pybind11::array_t<float> return_array(
    Eigen::Tensor<float, 3, Eigen::RowMajor> const &inp) {
    std::vector<ssize_t> shape(3);
    shape[0] = inp.dimension(0);
    shape[1] = inp.dimension(1);
    shape[2] = inp.dimension(2);
    return pybind11::array_t<float>(
        shape,  // shape
        {shape[1] * shape[2] * sizeof(float), shape[2] * sizeof(float),
         sizeof(float)},  // strides
        inp.data());       // data pointer
}

    enum {
        TASK_COMBINATION = 1,
        TASK_MORTALITY = 2
    };

    enum {
        // car,dme,hha,hosp,inp,out,snf
        CTYPE_DEN = 0,
        CTYPE_CAR,
        CTYPE_DME,
        CTYPE_HHA,
        CTYPE_HOSP,
        CTYPE_INP,
        CTYPE_OUT,
        CTYPE_SNF,
        CTYPE_TOTAL
    };

    int ctype_to_number (string const ctype) {
        if (ctype == "den") return CTYPE_DEN;
        if (ctype == "car") return CTYPE_CAR;
        if (ctype == "dme") return CTYPE_DME;
        if (ctype == "hha") return CTYPE_HHA;
        if (ctype == "hosp") return CTYPE_HOSP;
        if (ctype == "inp") return CTYPE_INP;
        if (ctype == "out") return CTYPE_OUT;
        if (ctype == "snf") return CTYPE_SNF;
        CHECK(false);
        return -1;
    }

    typedef Eigen::Matrix<float, Eigen::Dynamic, Eigen::Dynamic, Eigen::RowMajor> Matrix;
    typedef Eigen::VectorXf Vector;

    template <typename S>
    void serialize_matrix(S& s, Matrix &m) {
        int32_t rows = m.rows();
        int32_t cols = m.cols();
        int32_t total = rows * cols;
        s.value4b(rows);
        s.value4b(cols);
        s.value4b(total);
        CHECK(rows * cols == total);
        if ((rows != m.rows()) || (cols != m.cols())) {
            // input
            m = Matrix::Constant(rows, cols, 0);
        }
        float *ptr = m.data();
        for (int i = 0; i < total; ++i) {
            s.value4b(*ptr);
            ++ptr;
        }
    }

    template <typename S>
    void serialize_vector(S& s, Vector &m) {
        int32_t rows = m.rows();
        s.value4b(rows);
        if (rows != m.rows()) {
            // input
            m = Vector::Constant(rows, 0);
        }
        float *ptr = m.data();
        for (int i = 0; i < rows; ++i) {
            s.value4b(*ptr);
            ++ptr;
        }
    }

    // A merged line, representing all history of one beneficiery.
    static constexpr size_t LINE_BUF_SIZE = (16 * 1024 - 1)* 1024;

    boost::gregorian::date DATE_EPOCH(1800, 1, 1);

    int from_cms_date (int xxx) {
        int xxx_day = xxx % 100; xxx /= 100;
        int xxx_month = xxx % 100; xxx /= 100;
        int xxx_year = xxx % 10000; xxx /= 10000;
        CHECK(xxx == 0);
        return (boost::gregorian::date(xxx_year, xxx_month, xxx_day) - DATE_EPOCH).days();
    }

    int linear_20130101 = from_cms_date(20130101);
    int linear_20171231 = from_cms_date(20171231);


    int to_cms_date (int d) {
        boost::gregorian::date dd = DATE_EPOCH + boost::gregorian::date_duration(d);
        auto ymd = dd.year_month_day();
        return (ymd.year * 100 + ymd.month)*100+ymd.day;
    }

    enum {
        TYPE_UNKNOWN = 0,
        TYPE_STR = 1,
        TYPE_INT = 2,
        TYPE_FLOAT = 3,
        TYPE_DATE = 4
    };

    enum {
        ICD_UNKNOWN = 0,
        ICD_9 = 9,
        ICD_10 = 10
    };

    enum {
        AGGR_NONE = 0x0000,
        AGGR_COUNT = 0x0001,
        AGGR_MIN = 0x0002,
        AGGR_MAX = 0x0004,
        AGGR_SUM = 0x0008,
        AGGR_MEAN = 0x0010,
        AGGR_STD = 0x0020,
    };

    struct FeatureMeta {
        string name;
        int aggr = AGGR_NONE;
        FeatureMeta (string const &n, int a): name(n), aggr(a) {
        }
    };

    struct Value {
        bool is_none;
        uint8_t type;
        int8_t icd_source;
        uint8_t icd_version;
        char *begin;
        char *end;
        union {
            int64_t i_value;
            double f_value;
        };

        Value (): is_none(true), type(TYPE_UNKNOWN), icd_source(-1), icd_version(ICD_UNKNOWN), begin(nullptr), end(nullptr) {
            i_value = 0;
        }

        void add (Value const &other) {
            if (is_none) return;
            if (other.is_none) return;
            if (type == TYPE_INT) {
                if (other.type == TYPE_INT) {
                    i_value += other.i_value;
                }
                else if (other.type == TYPE_FLOAT) {
                    i_value += other.f_value;
                }
            }
            else if (type == TYPE_FLOAT) {
                if (other.type == TYPE_INT) {
                    f_value += other.i_value;
                }
                else if (other.type == TYPE_FLOAT) {
                    f_value += other.f_value;
                }
            }
        }
    };

    struct ICDCode {
        char text[8];
        uint8_t version;
        uint8_t source;

        ICDCode (Value const &v) {
            char *t = text;
            char *t_end = text + sizeof(text);
            CHECK(v.end > v.begin);
            CHECK(v.end - v.begin < sizeof(text));
            for (char const *p = v.begin; p < v.end; ++p) {
                *t = *p;
                ++t;
            }
            while (t < t_end) {
                *t = 0;
                ++t;
            }
            CHECK(v.icd_source >= 0);
            version = v.icd_version;
            source = v.icd_source;
        }
    };

    struct Node {
        string name;
        int parent;
        double prob;
        vector<int> children;
        int code;
    };

    struct State {
        vector<bool> use;
        vector<int> coder;
        vector<double> cls_prob;
        double entropy;
    };

    static inline double entropy (double p) {
        if (p == 0) return 0;
        return -p * std::log(p);
    }

    struct Tree: public vector<Node> {
        // at(0) 必须是根
        vector<int> topo_order;
        int codebook_size;
        unordered_map<string, int> lookup;
        unordered_map<string, vector<int>> icd10_lookup;

        void update_topo_order () {
            // 更新拓扑排序顺序topo_order
            vector<int> pending(size());
            topo_order.clear();
            topo_order.reserve(size());
            int total = 0;
            for (int i = 0; i < size(); ++i) {
                pending[i] = at(i).children.size();
                total += pending[i];
                if (pending[i] == 0) {
                    topo_order.push_back(i);
                }
            }
            CHECK(total + 1 == size());
            int next = 0;
            while (next < topo_order.size()) {
                int parent = at(topo_order[next]).parent;
                if (parent >= 0) {
                    --pending[parent];
                    if (pending[parent] == 0) {
                        topo_order.push_back(parent);
                    }
                }
                ++next;
            }
        }

        Tree (py::object py_tree): codebook_size(-1) {
            double total = 0;
            vector<int> use;
            py::list order = py::cast<py::list>(py_tree.attr("order"));
            for (auto handle: order) {
                int id = size();
                py::object py_node = py::cast<py::object>(handle);
                Node node;
                node.name = py::cast<string>(py_node.attr("name"));
                node.parent = py::cast<int>(py_node.attr("parent_id"));
                node.prob = py::cast<double>(py_node.attr("weight"));
                node.code = -1;
                push_back(node);
                py::object u = py_node.attr("use");
                if (!u.is_none()) {
                    if (py::cast<bool>(u)) {
                        use.push_back(id);
                    }
                }
                total += node.prob;
            }
            for (int i = 0; i < size(); ++i) {
                auto &node = at(i);
                node.prob /= total;
                if (node.parent >= 0) {
                    at(node.parent).children.push_back(i);
                }
            }
            //LOG(INFO) << "updating tree topo order";
            update_topo_order();
            //LOG(INFO) << "tree constructed";
            update_topo_order();

            if (use.size()) {
                //LOG(INFO) << "loading coder information.";
                CHECK(use[0] == 0);
                codebook_size = use.size();
                State state;
                state.use.resize(size(), false);
                vector<int> codes(size(), -1);
                for (int i = 0; i < use.size(); ++i) {
                    state.use[use[i]] = true;
                    codes[use[i]] = i;
                }
                update_coder(&state);
                int max_code = -1;
                for (int i = 0; i < size(); ++i) {
                    int coder = state.coder[i];
                    int c = codes[coder];
                    at(i).code = c;
                    if (c > max_code) {
                        max_code = c;
                    }
                }
                CHECK(max_code +1 == codebook_size);
                //LOG(INFO) << "codebook size " << codebook_size;
            }
            for (int i = 0; i < size(); ++i) {
                lookup[at(i).name] = i;
            }
            // update ICD10 mapping
            py::dict i10gem = py::cast<py::dict>(py_tree.attr("i10gem"));
            for (auto item: i10gem) {
                string key = py::cast<string>(item.first);
                auto &v = icd10_lookup[key];
                for (auto x: py::cast<py::list>(item.second)) {
                    string icd9 = py::cast<string>(x);
                    auto it = lookup.find(icd9);
                    CHECK(it != lookup.end());
                    v.push_back(it->second);
                }
            }
        }

        bool try_collect_icd9 (char const *text, vector<int> *l) const {
            auto it = lookup.find(string(text));
            if (it == lookup.end()) return false;
            int code = at(it->second).code;
            CHECK(code >= 0);
            l->push_back(code);
            return true;
        }

        bool try_encode_icd9 (char const *text, float *ft) const {
            auto it = lookup.find(string(text));
            if (it == lookup.end()) return false;
            int code = at(it->second).code;
            CHECK(code >= 0);
            ft[code] += 1;
            return true;
        }

        bool try_collect_icd10_shorten (char const *text_orig, vector<int> *l) const {
            char text[8];
            int n = strlen(text_orig);
            std::copy(text_orig, text_orig + n, text);
            for (; n >= 3; --n) {
                text[n] = 0;
                auto it = icd10_lookup.find(string(text));
                if (it == icd10_lookup.end()) continue;
                for (int cc: it->second) {
                    int code = at(cc).code;
                    CHECK(code >= 0);
                    l->push_back(code);
                }
                return true;
            }
            return false;
        }

        bool try_encode_icd10_shorten (char const *text_orig, float *ft) const {
            char text[8];
            int n = strlen(text_orig);
            std::copy(text_orig, text_orig + n, text);
            for (; n >= 3; --n) {
                text[n] = 0;
                auto it = icd10_lookup.find(string(text));
                if (it == icd10_lookup.end()) continue;
                for (int cc: it->second) {
                    int code = at(cc).code;
                    CHECK(code >= 0);
                    ft[code] += 1;
                }
                return true;
            }
            return false;
        }

        bool collect_codes (ICDCode const &code, vector<int> *l) const {
            if (code.version == ICD_9) {
                if (try_collect_icd9(code.text, l)) return true;
                return try_collect_icd10_shorten(code.text, l);
            }
            if (try_collect_icd10_shorten(code.text, l)) return true;
            return try_collect_icd9(code.text, l);
        }
            

        bool encode_sparse (ICDCode const &code, float *ft) const {
            if (code.version == ICD_9) {
                if (try_encode_icd9(code.text, ft)) return true;
                return try_encode_icd10_shorten(code.text, ft);
            }
            if (try_encode_icd10_shorten(code.text, ft)) return true;
            return try_encode_icd9(code.text, ft);
        }

        void update_coder_helper (State *state, int n, int coder) const {
            if (state->use[n]) {
                coder = n;
            }
            state->coder[n] = coder;
            for (int off: at(n).children) {
                update_coder_helper(state, off, coder);
            }
        }

        void update_coder (State *state) const {
            state->coder.clear();
            state->coder.resize(size(), -1);
            update_coder_helper(state, 0, -1);
            for (int i: state->coder) {
                CHECK(i >= 0);
            }
        }

        void update_closure_prob (State *state) const {
            CHECK(state->use.size() == size());
            CHECK(state->use[0]);
            auto &cls_prob = state->cls_prob;
            cls_prob.clear();
            cls_prob.resize(size(), 0);
            double entr = 0;    // entropy
            for (int offset: topo_order) {
                auto const &node = at(offset);
                cls_prob[offset] += node.prob;
                double p = cls_prob[offset];
                if (state->use[offset]) {
                    entr += entropy(p);
                }
                else {
                    cls_prob[node.parent] += p;
                }
            }
            state->entropy = entr;
        }

        void select (int K, int loops, double min_delta, State *ptr_state) const {
            if (K > size()) {
                K = size();
            }
            if (min_delta < 0) {
                min_delta = 0;
            }

            State &state = *ptr_state;
            state.use.clear();
            state.use.resize(size(), false);
            for (int i = 0; i < K; ++i) {
                state.use[i] = true;
            }
            LOG(INFO) << "updating coder";
            update_coder(&state);
            LOG(INFO) << "updating closure";
            update_closure_prob(&state);
            LOG(INFO) << "initial entropy: " << state.entropy;
            for (int x = 0; x < loops; ++x) {
                double best_remove_score = 0;
                int best_remove = -1;
                double best_add_score = 0;
                int best_add = -1;
                for (int n = 1; n < size(); ++n) { // node 0 is never changed,
                                                   // always used
                    if (state.use[n]) {
                        // currently used, what if removed
                        double prob = state.cls_prob[n];
                        int p = at(n).parent;
                        CHECK(p >= 0);
                        int coder = state.coder[p];
                        double orig = state.cls_prob[coder];
                        double change = entropy(orig + prob) - entropy(prob) - entropy(orig);
                        if ((best_remove < 0) || (change > best_remove_score)) {
                            best_remove_score = change;
                            best_remove = n;
                        }
                    }
                    else {
                        // currently not used, what if introduced
                        double orig = state.cls_prob[state.coder[n]];
                        double prob = state.cls_prob[n];
                        double change = entropy(orig - prob) + entropy(prob) - entropy(orig);
                        if ((best_add < 0) || (change > best_add_score)) {
                            best_add_score = change;
                            best_add = n;
                        }
                    }
                }
                double change = best_add_score + best_remove_score;
                if (change < min_delta) break;
                if (best_remove < 0) break;
                // make change
                // add best_add
                // remove best_remove
                CHECK(state.use[best_remove]);
                state.use[best_remove] = false;
                CHECK(!state.use[best_add]);
                state.use[best_add] = true;
                double orig_entropy = state.entropy;
                update_coder(&state);
                update_closure_prob(&state);
                change = state.entropy - orig_entropy;
                LOG(INFO) << "LOOP " << x << " ENTROPY " << state.entropy << " CHANGE " << change;
                if (change < min_delta) break;
            }
        }

        py::list try_select (int K, int loops, double min_delta) const {
            State state;
            select(K, loops, min_delta, &state);
            py::list l;
            for (int i = 0; i < size(); ++i) {
                if (state.use[i]) {
                    l.append(py::make_tuple(i, state.cls_prob[i]));
                }
            }
            return l;
        }
    };

    static inline int64_t rangetoll (char const *begin, char const *end) {
        // TODO: negative numbers
        int64_t v = 0;
        for (char const *p = begin; p < end; ++p) {
            v *= 10;
            char c = *p;
            if (!(c >= '0' && c <= '9')) throw std::domain_error("c");
            v += c - '0';
        }
        return v;
    }

    static inline char *find_next (char *begin, char *end, char v) {
        char *next;
        for (next = begin; next < end; ++next) {
            if (*next == v) return next;
        }
        return next;
    }


    int rangestrcmp (char const *begin, 
                 char const *end,
                 char const *s2) {
        for (char const *p = begin; p < end; ++p, ++s2) {
            if (s2[0] == 0) return 1;
            if (p[0] > s2[0]) return 1;
            else if (p[0] < s2[0]) return -1;
        }
        if (s2[0] != 0) return -1;
        return 0;
    }


    template <typename T>
    T cast (Value const &v);

    template <>
    string cast<string> (Value const &v) {
        return string(v.begin, v.end);
    }

    template <>
    int64_t cast<int64_t> (Value const &v) {
        return rangetoll(v.begin, v.end);
    }

    template <>
    double cast<double> (Value const &v) {
        char const *begin = v.begin;
        char const *end = v.end;
        char buf[250];
        CHECK(end > begin);
        size_t n = end - begin;
        CHECK(n <= 249);
        memcpy(buf, begin, n);
        buf[n] = 0;
        char *endptr;
        double d = strtod(buf, &endptr);
        CHECK(endptr == buf + n);
        return d;
    }

    template <>
    py::object cast<py::object> (Value const &v) {
        py::object obj;
        if (v.is_none) return py::none();
        if (v.type == TYPE_INT) return py::int_(v.i_value);
        if (v.type == TYPE_FLOAT) return py::float_(v.f_value);
        if (v.type == TYPE_STR) return py::str(string(v.begin, v.end));
        if (v.type == TYPE_DATE) return py::int_(v.i_value);
        CHECK(0);
        return py::none();
    }

    vector<string> ICD_FIELD_PREFIXES{
        "ADMTG_DGNS_CD",
        "FST_DGNS_E_CD",
        "ICD_DGNS_CD",
        "ICD_DGNS_E_CD",
        "ICD_PRCDR_CD",
        "LINE_ICD_DGNS_CD",
        "PRNCPAL_DGNS_CD",
        "RSN_VISIT_CD"
    };

    int constexpr ICD_SOURCE_PRCDR = 4;

    // Describes field in raw data
    struct RawFieldDesc {
        int offset;
        int8_t icd_source;
        int version_offset;
        uint8_t type;
        string name;
        RawFieldDesc (int o, int t, string const &n): offset(o), type(t), name(n) {
            CHECK(t < numeric_limits<uint8_t>::max());
            version_offset = -1;
            icd_source = -1;
        }
    };

    struct RawTableDesc {
        int tid;    // table ID
                    // multiple formats might corresponds to the same table
                    // different in year only
        int year;
        string ctype;
        string sub;
        string name;
        bool use;   // whether the table is used
        vector<RawFieldDesc> fields;
        map<string, RawFieldDesc const *> lookup;
        int pid_field = -1; // DESY_SORT_KEY
        int claim_no_field = -1;

        RawTableDesc (py::handle v) {
            py::tuple tup = py::cast<py::tuple>(v);
            tid = py::cast<int>(tup[0]);
            year = py::cast<int>(tup[1]);
            ctype = py::cast<string>(tup[2]);
            if (!tup[3].is_none()) {
                sub = py::cast<string>(tup[3]);
                name = ctype + "_" + sub;
            }
            else {
                CHECK(ctype == "den");
                name = ctype;
            }
            pid_field = py::cast<int>(tup[4]);
            use = py::cast<bool>(tup[6]);
            py::list py_cols = tup[5];
            for (auto const &f: py_cols) {
                py::object ff = py::cast<py::object>(f);
                string long_name = py::cast<string>(ff.attr("long_name"));
                int type = py::cast<int>(ff.attr("type"));
                fields.emplace_back(fields.size(), type, long_name);
            }
            CHECK(fields[pid_field].name == "DESY_SORT_KEY");
            for (int i = 0; i < fields.size(); ++i) {
                lookup[fields[i].name] = &fields[i];
                if (fields[i].name == "CLAIM_NO") {
                    claim_no_field = i;
                }
            }
            if (claim_no_field < 0) {
                CHECK(ctype == "den") << "NO_CLAIM_NO " << ctype << " " << sub << " " << year;
            }
            CHECK(ICD_FIELD_PREFIXES[ICD_SOURCE_PRCDR] == "ICD_PRCDR_CD");
            // set up ICD version reference
            for (int i = 0; i < fields.size(); ++i) {
                string const &name = fields[i].name;
                for (int source = 0; source < ICD_FIELD_PREFIXES.size(); ++source) {
                    if (name.find(ICD_FIELD_PREFIXES[source]) ==  0) {
                        fields[i].icd_source = source;
                        CHECK(fields[i].icd_source == source);
                    }
                }
                auto off = name.find("_VRSN_CD");
                if (off == name.npos) {
                    CHECK(name.find("_VRSN_") == name.npos);
                    continue;
                }
                string novrsn = name;
                novrsn.replace(off, 5, "");
                auto it = lookup.find(novrsn);
                if (it == lookup.end()) {
                    LOG(WARNING) << "No corresponding cd found for " << name;
                }
                else {
                    auto &cd_field = fields[it->second->offset];
                    CHECK(cd_field.offset == it->second->offset);
                    cd_field.version_offset = i;
                    //LOG(INFO) << this->name << " version of " << cd_field.name << ":" << cd_field.offset << " is " << fields[i].name << ":" << cd_field.version_offset;
                }
            }
        }
    };

    struct RawDesc {
        vector<RawTableDesc> formats;
        map<string, int> lookup;    // name -> tid

        RawDesc (py::object raw_loader) {
            py::list py_formats = raw_loader.attr("formats");
            py::list names = raw_loader.attr("names");
            for (auto const &v: names) {
                lookup[py::cast<string>(v)] = lookup.size();
            }
            for (auto const &v: py_formats) {
                formats.emplace_back(v);
            }
        }

        RawTableDesc const *find_first_table_by_name (string const &name) const {
            auto it = lookup.find(name);
            CHECK(it != lookup.end());
            for (auto &f: formats) {
                if (f.tid == it->second) {
                    CHECK(f.name == name);
                    return &f;
                }
            }
            CHECK(0);
            return nullptr;
        }

    };

    struct Row {
        //int table_offset = -1;
        int64_t pid = -1;
        int64_t claim_no = -1;
        vector<Value> fields;

        Row (char *begin, char *end, RawTableDesc const &desc) {
            //table_offset = desc.offset;
            fields.resize(desc.fields.size());
            string text(begin, end);
            char *next = begin;
            for (int i = 0; i < fields.size(); ++i) { CHECK(next <= end) << desc.name << " " << desc.year << " " << text;
                auto const &field_desc = desc.fields[i];
                auto &field = fields[i];
                field.begin = next;
                field.end = find_next(next, end, ',');

                field.type = field_desc.type;
                field.i_value = 0;
                field.is_none = true;
                field.icd_source = field_desc.icd_source;
                field.icd_version = ICD_UNKNOWN;
                if (field.begin < field.end) {
                    *field.end = 0;
                    field.is_none = false;
                    if (field.type == TYPE_INT) {
                        //char *end;
                        try {
                            field.i_value = cast<int64_t>(field);
                        }
                        catch (...) {   // 1E4
                            double x = cast<double>(field);
                            field.i_value = x;
                            CHECK(field.i_value == x);
                        }
                        /*
                        if (end < field.end) {
                            CHECK(*end == '.');
                            ++end;
                            while (end < field.end) {
                                CHECK(*end == '0') << field_desc.name << " " << string(field.begin, field.end);
                                ++end;
                            }
                        }
                        else */ //CHECK(end == field.end) << string(field.begin, field.end);
                    }
                    else if (field.type == TYPE_FLOAT) {
                        field.f_value = cast<double>(field);
                        //char *end;
                        //field.f_value = cast<double>(field);//strtod(field.begin, &end);
                        //CHECK(end == field.end);
                        // if no conversion is done, 
                        // 0 is returned and end == field.begin
                    }
                    else if (field.type == TYPE_DATE) {
                        CHECK(field.end - field.begin == 8);
                        //char *end;
                        //int64_t xxx = strtoll(field.begin, &end, 10);
                        //CHECK(end == field.end) << string(field.begin, field.end);
                        field.i_value = cast<int64_t>(field);
                        //field.i_value = xxx; //from_cms_date(xxx);
                    }
                }
                next = field.end + 1;
            }
            CHECK(next == end + 1) << (void *)next << " " << (void *)end;
            // patch version
            for (int i = 0; i < fields.size(); ++i) {
                auto const &field_desc = desc.fields[i];
                if (field_desc.version_offset < 0) continue;
                auto const &vf = fields[field_desc.version_offset];
                char v_code = 0;
                if (vf.begin < vf.end) {
                    v_code = vf.begin[0];
                }
                // https://www.resdac.org/cms-data/variables/claim-diagnosis-code-i-diagnosis-version-code-icd-9-or-icd-10-ffs
                // blank is version 9
                if (v_code == '9' || v_code == ' ' || v_code == 0) {
                    fields[i].icd_version = ICD_9;
                }
                else if (v_code == '0') {
                    fields[i].icd_version = ICD_10;
                }
                else LOG(ERROR) << "unknown ICD version code: " << v_code;
            }

            CHECK(desc.pid_field >= 0);

            if (desc.pid_field >= 0) {
                CHECK(desc.pid_field < fields.size());
                auto const &f = fields[desc.pid_field];
                //CHECK(f.type == TYPE_INT) << cast<string>(f);
                CHECK(!f.is_none);
                pid = cast<int64_t>(f);
            }

            if (desc.claim_no_field >= 0) {
                CHECK(desc.claim_no_field < fields.size());
                auto const &f = fields[desc.claim_no_field];
                CHECK(f.type == TYPE_INT);
                CHECK(!f.is_none);
                claim_no = f.i_value;
            }
        }
    };


    // corresponding to python normalized case
    class Record;

    struct Field {
        Value value;
        vector<Record> sub;
    };

    struct Record {
        vector<Field> fields;
    };

    struct Table {
        vector<Record> records;
    };

    
    enum {
        FIELD_ATOMIC = 1,
        FIELD_MULTIPLE = 2,
        FIELD_SUBTABLE = 3
    };

    struct FieldInputSpecWrapper {
        int type;
        string name;
        vector<string> subfields;

        RawTableDesc const *input_table = nullptr;
        RawFieldDesc const *input_field = nullptr;
        vector<RawFieldDesc const *> input_subfields; // for subtable
        vector<vector<RawFieldDesc const *>> input_subfields_groups; // for multiple

        FieldInputSpecWrapper (string const &field_name, py::handle v, RawDesc const &raw_desc, RawTableDesc const * table_version) {
            CHECK(table_version);
            py::list li = py::cast<py::list>(v);
            if (!li[0].is_none()) {
                name = py::cast<string>(li[0]);
            }
            if (!li[1].is_none()) {
                py::list subs = li[1];
                for (auto const &h: subs) {
                    subfields.push_back(py::cast<string>(h));
                }
            }
            if (name.size() > 0 && subfields.size() == 0) {
                type = FIELD_ATOMIC;
                input_table = table_version;
                auto it = input_table->lookup.find(name);
                CHECK(it != input_table->lookup.end());
                input_field = it->second;
            }
            else {
                CHECK(subfields.size() > 0);

                if (name.size() > 0) {
                    type = FIELD_SUBTABLE;
                    input_table = raw_desc.find_first_table_by_name(name);
                    for (auto const &n: subfields) {
                        auto it = input_table->lookup.find(n);
                        CHECK(it != input_table->lookup.end()) << input_table->name << " cannot find " << n;
                        input_subfields.push_back(it->second);
                    }
                }
                else {
                    type = FIELD_MULTIPLE;
                    input_table = table_version;
                    for (int xxx = 1; ; ++xxx) {
                        vector<RawFieldDesc const *> sf;
                        string suffix = fmt::sprintf("%d", xxx);
                        for (auto const &n: subfields) {
                            auto it = input_table->lookup.find(n + suffix);
                            if (it == input_table->lookup.end()) break;
                            sf.push_back(it->second);
                        }
                        if (sf.size() < subfields.size()) {
                            CHECK(sf.size() == 0);
                            break;
                        }
                        input_subfields_groups.emplace_back();
                        input_subfields_groups.back().swap(sf);
                    }
                    //LOG(INFO) << "GROUP " << field_name << " found " << input_subfields_groups.size();
                }
            }
        }
    };

    struct FieldSpecWrapper {
        string name;
        vector<string> subfields;
        vector<FieldInputSpecWrapper> input_specs;
        py::object ctor;
        FieldSpecWrapper (py::handle v, RawDesc const &raw_desc, vector<RawTableDesc const *> const &table_versions) {
            py::list l = py::cast<py::list>(v);
            name = py::cast<string>(l[0]);
            if (!l[1].is_none()) {
                for (auto const &&s: py::cast<py::list>(l[1])) {
                    subfields.push_back(py::cast<string>(s));
                }
            }
            for (auto v: table_versions) {
                CHECK(v);
            }
            for (auto const &s: py::cast<py::list>(l[2])) {
                CHECK(input_specs.size() < table_versions.size());
                input_specs.emplace_back(name, s, raw_desc, table_versions[input_specs.size()]);
            }
            ctor = l[4];
        }
    };

    struct SpecWrapper {
        bool is_claim;
        bool is_snf_inp;
        string ctype;
        int ctype_number;
        vector<RawTableDesc const *> versions;       // table_index
        vector<FieldSpecWrapper> fields;
        py::object ctor;
        SpecWrapper (py::handle v, RawDesc const &raw_desc) {
            py::list spec = py::cast<py::list>(v);
            CHECK(py::len(spec) == 4);
            ctype = py::cast<string>(spec[0]);
            ctype_number = ctype_to_number(ctype);
            is_claim = (ctype != "den");
            is_snf_inp = (ctype == "snf") || (ctype == "inp");
            py::list py_versions = spec[1];
            for (auto const &v: py_versions) {
                versions.push_back(raw_desc.find_first_table_by_name(py::cast<string>(v)));
            }
            py::list py_fields = spec[2];
            for (auto const &f: py_fields) {
                fields.emplace_back(f, raw_desc, versions);
            }
            ctor = spec[3];
        }
    };

    class ClaimFieldDesc;
    struct Claim;
    struct Demo;

    class Xtor {
        mutable unordered_map<string, int> unknown;
    protected:
        void track (Value const &v) const {
            unknown[cast<string>(v)] += 1;
        }
    public:
        virtual ~Xtor () {}
        virtual int size () const {
            return 0;
        }
        virtual void get_meta (string const &, vector<FeatureMeta> *) const {
        };
        virtual float *extract (Value const &v, float *ft, ClaimFieldDesc const &) const {
            return ft;
        }
        virtual float *extract (Claim const &, Demo const &, float *ft, vector<ClaimFieldDesc> const &descs, int) const {
            CHECK(0);
            return ft;
        }
    };

    struct ClaimFieldDesc {
        string name;
        vector<string> input_names;
        vector<int> input_table_offsets;    // offset from each table
                                            // use input_names to lookup
        Xtor *xtor;
        ClaimFieldDesc (string const &n, vector<string> const &ins, Xtor *x = nullptr)
            : name(n), input_names(ins), xtor(x) {
            if (input_names.empty()) {
                input_names.push_back(name);
            }
            if (!xtor) {
                xtor = new Xtor();
            }
        }
    };

    struct DemoOrClaim {
        SpecWrapper const *spec;
        vector<Value> fields;

        string ctype () const {
            return spec->ctype;
        }
        /*
        py::list get_py () const {
            py::list l;
            for (auto const &v: fields) {
                l.append(cast<py::object>(v));
            }
            return l;
        }
        */
    };

    struct Claim: public DemoOrClaim {
        int64_t is_snf_inp;
        int64_t CLAIM_NO;
        int64_t CLM_THRU_DT;
        int64_t CLM_ADMSN_DT;
        int merge_count = 1;
        vector<ICDCode> icd_codes;   //  PRNCPAL_DGNS_CD

        py::object getattr (string const &name) const;

        py::list get_icd_codes () const {
            py::list cc;
            for (auto const &code: icd_codes) {
                cc.append(py::make_tuple(string(code.text), code.version, ICD_FIELD_PREFIXES[code.source]));
            }
            return cc;
        }

        std::pair<int, int> time_range () const{
            int begin = CLM_ADMSN_DT;
            int end = CLM_THRU_DT;
            if (begin < 0) begin = end;
            return std::make_pair(begin, end);
        }

    };

    struct Demo: public DemoOrClaim {
        int year; //REFERENCE_YEAR;
        int64_t DATE_OF_DEATH;
        py::object getattr (string const &name) const;
    };

    bool operator < (Claim const &c1, Claim const &c2) {
        return c1.CLM_THRU_DT < c2.CLM_THRU_DT;
    }

    bool operator < (Demo const &c1, Demo const &c2) {
        return c1.year < c2.year;
    }


    class XtorSkip: public Xtor {
    };

    class XtorDisable: public Xtor {
    };

    class XtorCopy: public Xtor {
        int aggr;
    public:
        XtorCopy (int aggr_ = AGGR_MEAN): aggr(aggr_) {
        }

        virtual int size () const {
            return 1;
        }
        virtual void get_meta (string const &n, vector<FeatureMeta> *v) const {
            v->emplace_back(n, aggr);
        };
        virtual float *extract (Value const &v, float *ft, ClaimFieldDesc const &desc) const {
            *ft = numeric_limits<float>::quiet_NaN();
            if (!v.is_none) {
                if (v.type == TYPE_INT) {
                    *ft = v.i_value;
                }
                else if (v.type == TYPE_FLOAT) {
                    *ft = v.f_value;
                }
                else CHECK(0);
            }
            return ft + 1;
        }
    };

    class XtorOneHot: public Xtor {
        int dim;
        vector<pair<char const *, int>> dict;
    public:
        XtorOneHot (vector<char const *> const &d) {
            for (int i = 0; i < d.size(); ++i) {
                dict.emplace_back(d[i], i);
            }
            dim = d.size();
        }
        virtual int size () const {
            return dim;
        }
        virtual void get_meta (string const &n, vector<FeatureMeta> *v) const {
            vector<char const *> names(dim, nullptr);
            for (auto const &p: dict) {
                names[p.second] = p.first;
            }
            for (int i = 0; i < names.size(); ++i) {
                if (names[i]) {
                    v->emplace_back(fmt::sprintf("%s_%s", n, names[i]), AGGR_SUM);
                }
                else {
                    v->emplace_back(fmt::sprintf("%s.%d", n, i), AGGR_SUM);
                }
            }
        };
        virtual float *extract (Value const &v, float *ft, ClaimFieldDesc const &desc) const {
            std::fill(ft, ft + dim, 0);
            if (!v.is_none) {
                bool found = false;
                for (auto p: dict) {
                    if (rangestrcmp(v.begin, v.end, p.first) == 0) {
                        ft[p.second] += 1;
                        found = true;
                    }
                }
                if (!found) track(v);
            }
            return ft + dim;
        }
    };

    class XtorMdcrStusCd: public Xtor {
        enum {
            AGE = 0,
            DISABLE = 1,
            ESRD = 2
        };
    public:
        virtual int size () const {
            return 3;
        }
        virtual void get_meta (string const &n, vector<FeatureMeta> *v) const {
            v->emplace_back("MdcrStusAge", AGGR_MAX);
            v->emplace_back("MdcrStusDisable", AGGR_MAX);
            v->emplace_back("MdcrStusESRD", AGGR_MAX);
        }
        virtual float *extract (Value const &v, float *ft, ClaimFieldDesc const &desc) const {
            ft[AGE] = ft[DISABLE] = ft[ESRD] = 0;
            if (!v.is_none) try {
                int i_value = cast<int64_t>(v);
                switch (i_value) {
                    case 10:
                        ft[AGE] = 1;
                        break;
                    case 11:
                        ft[AGE] = 1;
                        ft[ESRD] = 1;
                        break;
                    case 20:
                        ft[DISABLE] = 1;
                        break;
                    case 21:
                        ft[DISABLE] = 1;
                        ft[ESRD] = 1;
                        break;
                    case 31:
                        ft[ESRD] = 1;
                        break;
                    default:
                        track(v);
                }
            }
            catch (...) {
                track(v);
            }
            return ft + 3;
        }
    };

    class XtorRsnCurr: public Xtor {
        enum {
            AGE = 0,
            DISABLE = 1,
            ESRD = 2
        };
    public:
        virtual int size () const {
            return 3;
        }
        virtual void get_meta (string const &n, vector<FeatureMeta> *v) const {
            v->emplace_back("RsnAge", AGGR_MAX);
            v->emplace_back("RsnDisable", AGGR_MAX);
            v->emplace_back("RsnESRD", AGGR_MAX);
        }
        virtual float *extract (Value const &v, float *ft, ClaimFieldDesc const &desc) const {
            ft[AGE] = ft[DISABLE] = ft[ESRD] = 0;
            if (!v.is_none) try {
                int i_value = cast<int64_t>(v);
                switch (i_value) {
                    case 0:
                        ft[AGE] = 1;
                        break;
                    case 1:
                        ft[DISABLE] = 1;
                        break;
                    case 2:
                        ft[ESRD] = 1;
                        break;
                    case 3:
                        ft[DISABLE] = 1;
                        ft[ESRD] = 1;
                        break;
                    default:
                        track(v);
                }
            }
            catch (...) {
                track(v);
            }
            return ft + 3;
        }
    };


    enum {
        DEMO_DESY_SORT_KEY = 0,
        DEMO_DATE_OF_DEATH = 17,
        DEMO_REFERENCE_YEAR = 18,
        CLAIM_CLAIM_NO = 0,
        CLAIM_CLM_THRU_DT = 1,
        CLAIM_CLM_PMT_AMT = 3,
        CLAIM_DOB_DT = 4,
        CLAIM_CLM_TOT_CHRG_AMT = 8,
        CLAIM_CLM_ADMSN_DT = 14
    };

    /*
    class XtorClaimAge: public Xtor {
    public:
        virtual int size () const {
            return 1;
        }
        virtual void get_meta (string const &n, vector<FeatureMeta> *v) const {
            v->push_back("Age");
        }
        virtual float *extract (Claim const &claim, Demo const &, float *ft, vector<ClaimFieldDesc> const &descs, int cutoff) const {
            Value const &thru = claim.fields[CLAIM_CLM_THRU_DT];
            Value const &dob = claim.fields[CLAIM_DOB_DT];
            *ft = numeric_limits<float>::quiet_NaN();
            if (!(thru.is_none || dob.is_none)) {
                CHECK(thru.type == TYPE_DATE || thru.type == TYPE_INT) << " " << thru.type;
                CHECK(dob.type == TYPE_DATE || dob.type == TYPE_INT) << " " << dob.type;
                bool good = true;
                int a, b;
                try {
                    a = from_cms_date(thru.i_value);
                }
                catch (...) {
                    good = false;
                    LOG(ERROR) << "CLAIM THRU " << claim.CLAIM_NO << " " << claim.CLM_THRU_DT << " " << thru.i_value;
                }
                try {
                    b = from_cms_date(dob.i_value);
                }
                catch (...) {
                    good = false;
                    LOG(ERROR) << "CLAIM DOB " << claim.CLAIM_NO << " " << claim.CLM_THRU_DT << " " << dob.i_value;
                }
                if (good) {
                    *ft = a - b;
                }
            }
            return ft+1;
        }
    };
    */

    class XtorClaimAge: public Xtor {
    public:
        virtual int size () const {
            return 3;
        }
        virtual void get_meta (string const &n, vector<FeatureMeta> *v) const {
            v->emplace_back("ClaimAge", AGGR_NONE);
            v->emplace_back("ClaimDuration", AGGR_NONE);
            v->emplace_back("ClaimCount", AGGR_NONE);
        }
        virtual float *extract (Claim const &claim, Demo const &, float *ft, vector<ClaimFieldDesc> const &descs, int cutoff) const {
            ft[0] = from_cms_date(cutoff) - from_cms_date(claim.CLM_THRU_DT);
            auto range = claim.time_range();
            ft[1] = from_cms_date(range.second) - from_cms_date(range.first);
            ft[2] = claim.merge_count;
            //CHECK(*ft > 0); // TODO
            return ft+3;
        }
    };

    class XtorSparseICD: public Xtor {
        Tree const *tree;
        int dim;
    public:
        XtorSparseICD (Tree const *t): tree(t) {
            dim = tree->codebook_size;
        }
        virtual int size () const {
            return dim;
        }
        virtual void get_meta (string const &n, vector<FeatureMeta> *v) const {
            for (int i = 0; i < dim; ++i) {
                v->emplace_back(fmt::sprintf("ICD_%d", i), AGGR_SUM);
            }
        }
        virtual float *extract (Claim const &claim, Demo const &, float *ft, vector<ClaimFieldDesc> const &descs, int cutoff) const {
            std::fill(ft, ft+dim, 0);
            for (ICDCode const &icd: claim.icd_codes) {
                if (icd.source == ICD_SOURCE_PRCDR) continue;
                tree->encode_sparse(icd, ft);
            }
            for (int i = 0; i < dim; ++i) {
                if (ft[i] > 0) {
                    ft[i] = 1;
                }
            }
            return ft+dim;
        }
    };

    // Feature Engineering
    vector<ClaimFieldDesc> DEMO_FIELD_DESCS {
        {"DESY_SORT_KEY", {}, new XtorSkip()},
        {"STATE_CODE", {}, new XtorSkip()},
        {"COUNTY_CODE", {}, new XtorSkip()},
        {"SEX_CODE", {}, new XtorOneHot({"1", "2"})},
        {"RACE_CODE", {}, new XtorOneHot({"1","2","3","4","5","6"})},
        {"AGE", {}, new XtorCopy()},
        {"ORIG_REASON_FOR_ENTITLEMENT", {}, new XtorSkip()},
        {"CURR_REASON_FOR_ENTITLEMENT", {}, new XtorSkip()}, // duplidate
        //{"ESRD_INDICATOR", {}, new XtorSkip()},  // 0 for no or 'Y' for yes
        {"ESRD_INDICATOR", {}, new XtorOneHot({"Y"})},  // 0 for no or 'Y' for yes
        {"MEDICARE_STATUS_CODE", {}, new XtorSkip()},   // duplicate
        {"PART_A_TERMINATION_CODE", {}, new XtorDisable()},
        {"PART_B_TERMINATION_CODE", {}, new XtorDisable()},
        {"HI_COVERAGE", {}, new XtorDisable()},
        {"SMI_COVERAGE", {}, new XtorDisable()},
        {"HMO_COVERAGE", {}, new XtorDisable()},
        {"STATE_BUY_IN_COVERAGE", {}, new XtorDisable()},
        {"VALID_DATE_OF_DEATH_SWITCH", {}, new XtorDisable()},
        {"DATE_OF_DEATH", {}, new XtorDisable()},
        {"REFERENCE_YEAR", {}, new XtorSkip()},
    };

    // Feature Engineering
    vector<ClaimFieldDesc> CLAIM_FIELD_DESCS {
        {"CLAIM_NO", {}, new XtorSkip()},            // int
        {"CLM_THRU_DT", {}, new Xtor()},         // int, 8 digits sorted as int

        //{"NCH_CLM_TYPE_CD", {}, new XtorIntOneHot({10,20,30,40,50,60,61,71,72,81,82})},     // int
        {"NCH_CLM_TYPE_CD", {}, new XtorSkip()},    //  和CLM_FAC_TYPE_CD重复
        {"CLM_PMT_AMT", {}, new XtorCopy()},
        {"DOB_DT", {}, new XtorSkip()}, //new XtorCopy()},              // int TODO
        // https://www.resdac.org/cms-data/variables/lds-age-category
        {"GNDR_CD", {}, new XtorSkip()}, //XtorOneHot({"1", "2"})},  // int, 0: unknown, 1: male, 2: female
        {"BENE_RACE_CD", {}, new XtorSkip()}, //new XtorOneHot({"1","2","3","4","5","6"})},   // int
        // 0-6: UNKNOWN, WHITE, BLACK, OTHER, ASIAN, HISPANIC, NATIVE
        {"CWF_BENE_MDCR_STUS_CD", {}, new XtorMdcrStusCd()},   // int
        {"CLM_TOT_CHRG_AMT", vector<string>{"CLM_TOT_CHRG_AMT", "NCH_CARR_CLM_SBMTD_CHRG_AMT"}, new XtorCopy()},
        ////// only for: hha,hosp,inp,out,snf
        {"PRVDR_NUM", {}, new XtorSkip()},
        {"CLM_FAC_TYPE_CD", {}, new XtorOneHot({"1","2","3","4","6","7","8"})},
        // https://bluebutton.cms.gov/resources/variables/clm_fac_type_cd/
        // 
        {"CLM_SRVC_CLSFCTN_TYPE_CD", {}, new XtorSkip()},
        // https://bluebutton.cms.gov/resources/variables/clm_srvc_clsfctn_type_cd/
        {"CLM_FREQ_CD", {}, new XtorSkip()},
        // https://bluebutton.cms.gov/resources/variables/clm_freq_cd/
        {"PTNT_DSCHRG_STUS_CD", {}, new XtorSkip()},
        // https://bluebutton.cms.gov/resources/variables/ptnt_dschrg_stus_cd/
        ////// only for: hha, hosp, inp, snf
        {"CLM_ADMSN_DT", {"CLM_ADMSN_DT", "CLM_HOSPC_START_DT_ID"}, new XtorSkip()},
        ////// only for: hosp, inp, snf
        {"NCH_BENE_DSCHRG_DT", {}, new XtorSkip()},
        ////// only for: inp, snf
        {"CLM_IP_ADMSN_TYPE_CD", {}, new XtorSkip()},
        {"CLM_SRC_IP_ADMSN_CD", {}, new XtorSkip()}
    };

    // Feature Engineering
    vector<Xtor *> CLAIM_LEVEL_XTORS {
        new XtorClaimAge(),
    };


    class Case;

    struct CaseLoaderWrapper {
        RawDesc raw_desc;

        py::object case_ctor;
        vector<SpecWrapper> specs;
        vector<ClaimFieldDesc> claim_fields;
        vector<ClaimFieldDesc> demo_fields;
        vector<Xtor *> claim_xtors;
        int demo_dimensions;
        vector<FeatureMeta> feature_meta;
        Tree icd9_tree;

        CaseLoaderWrapper (py::object obj)
            : raw_desc(obj.attr("loader")),
            icd9_tree(obj.attr("icd9")) {
            py::object normer = obj.attr("normer");
            case_ctor = normer.attr("case_ctor");
            py::list py_specs = normer.attr("specs");
            for (auto const &v: py_specs) {
                specs.emplace_back(v, raw_desc);
            }
            claim_fields = CLAIM_FIELD_DESCS;
            CHECK(claim_fields[CLAIM_CLAIM_NO].name == "CLAIM_NO");
            CHECK(claim_fields[CLAIM_CLM_THRU_DT].name == "CLM_THRU_DT");
            CHECK(claim_fields[CLAIM_CLM_PMT_AMT].name == "CLM_PMT_AMT");
            CHECK(claim_fields[CLAIM_DOB_DT].name == "DOB_DT");
            CHECK(claim_fields[CLAIM_CLM_TOT_CHRG_AMT].name == "CLM_TOT_CHRG_AMT");
            CHECK(claim_fields[CLAIM_CLM_ADMSN_DT].name == "CLM_ADMSN_DT");
            for (auto &f: claim_fields) {
                // 查找本table对应的字段
                for (auto const &t: specs) {
                    int off = -1;
                    for (int xx = 0; xx < t.fields.size(); ++xx) {
                        for (auto const &name: f.input_names) {
                            if (t.fields[xx].name == name) {
                                CHECK(off < 0);
                                off = xx;
                            }
                        }
                    }
                    f.input_table_offsets.push_back(off);
                }
            }
            demo_fields = DEMO_FIELD_DESCS;
            CHECK(demo_fields[DEMO_DESY_SORT_KEY].name == "DESY_SORT_KEY");
            CHECK(demo_fields[DEMO_REFERENCE_YEAR].name == "REFERENCE_YEAR");
            for (auto &f: demo_fields) {
                // 查找本table对应的字段
                for (auto const &t: specs) {
                    int off = -1;
                    for (int xx = 0; xx < t.fields.size(); ++xx) {
                        for (auto const &name: f.input_names) {
                            if (t.fields[xx].name == name) {
                                CHECK(off < 0);
                                off = xx;
                            }
                        }
                    }
                    if (!t.is_claim) CHECK(off >= 0);
                    f.input_table_offsets.push_back(off);
                }
            }
            claim_xtors = CLAIM_LEVEL_XTORS;
            // Feature Engineering
            claim_xtors.push_back(new XtorSparseICD(&icd9_tree));
            //LOG(INFO) << "meta data loaded.";
            for (auto &f: demo_fields) {
                f.xtor->get_meta(f.name, &feature_meta);
            }
            demo_dimensions = feature_meta.size();
            for (auto &f: claim_fields) {
                f.xtor->get_meta(f.name, &feature_meta);
            }
            for (Xtor const *xtor: claim_xtors) {
                xtor->get_meta("", &feature_meta);
            }
            //LOG(INFO) << "claim feature size: " << feature_meta.size();
        };
        Case *load (py::bytes, bool copy) const;
        Case *load (py::bytes buf) const {
            return load(buf, true);
        }
        py::object get_norm_case (Case *) const;
        py::dict get_reduced_case (Case *) const;
        Matrix* get_extracted_case (Case *, int time) const;
        Vector* get_aggr_case (Case *, int time) const;
        py::list get_aggr_case_columns () const;
        py::dict bulk_load_features (py::list py_paths,
                            py::list py_pick) const;
        py::dict bulk_load_aggr_features (py::list py_paths,
                            py::list py_pick) const;
        py::list feature_columns () const {
            py::list r;
            for (auto const &m: feature_meta) {
                r.append(py::str(m.name));
            }
            return r;
        }
        py::list demo_columns () const {
            py::list r;
            for (auto const &m: demo_fields) {
                r.append(py::str(m.name));
            }
            return r;
        }
        py::list claim_columns () const {
            py::list r;
            for (auto const &m: claim_fields) {
                r.append(py::str(m.name));
            }
            return r;
        }
        py::list aggr_feature_columns () const {
            py::list r;
            for (auto const &m: feature_meta) {
                r.append(py::str(m.name));
            }
            LOG(INFO) << "CCC";
            return r;
        }

    };

    struct Case {
        int64_t pid;
        int label = -1;
        double predict = -1;
        vector<Table> ctypes;
        vector<Demo> demos;
        vector<Claim> claims;
        vector<char> buf;

        void drop_claims_on_thru_dt (int thru_dt) {
                vector<Claim> n;
                for (auto &c: claims) {
                        if (c.CLM_THRU_DT == thru_dt) {
                                continue;
                        }
                        n.emplace_back(std::move(c));
                }
                claims.swap(n);
        }

        void cutoff (int co) {
            int cutoff_year = co / 10000;
            for (int i = 0; i < demos.size(); ++i) {
                if (demos[i].year > cutoff_year) {
                    demos.resize(i);
                    break;
                }
            }
            for (int i = 0; i < claims.size(); ++i) {
                if (claims[i].CLM_THRU_DT >= co) {
                    claims.resize(i);
                    break;
                }
            }
        }

        void load_sub_table (vector<vector<Row>> const &tables, FieldInputSpecWrapper const &spec, int64_t claim_no, Field *out_field) {
            CHECK(spec.input_table);
            auto &table = tables[spec.input_table->tid];
            for (auto &row: table) {
                if (row.claim_no != claim_no) continue;
                Record &record = out_field->sub.emplace_back();
                record.fields.reserve(spec.input_subfields.size());
                for (RawFieldDesc const *f: spec.input_subfields) {
                    Field &out_f = record.fields.emplace_back();
                    out_f.value = row.fields[f->offset];
                }
            }
        }

        void load_column_groups (Row const &row, FieldInputSpecWrapper const &spec, Field *out_field) {
            int old_non_nan = 0;
            for (auto const &group: spec.input_subfields_groups) {
                Record &record = out_field->sub.emplace_back();
                record.fields.reserve(group.size());
                int non_nan = 0;
                for (RawFieldDesc const *f: group) {
                    Field &out_f = record.fields.emplace_back();
                    out_f.value = row.fields[f->offset];
                    if (!out_f.value.is_none) {
                        ++non_nan;
                    }
                }
                if (non_nan == 0) {
                    out_field->sub.pop_back();
                    break;
                }
                if (old_non_nan == 0) old_non_nan = non_nan;
                else CHECK(old_non_nan == non_nan);
            }
        }

        Case (char *begin, char *end, CaseLoaderWrapper const &rc_wrapper, bool copy = true) {
            CHECK(end > begin);
            if (end[-1] == '\n') --end;
            if (copy) {
                size_t sz = end - begin;
                buf.resize(sz + 1);
                buf.back() = 0;
                std::copy(begin, end, buf.begin());
                begin = &buf[0];
                end = begin + sz;
            }
            vector<vector<Row>> tables(rc_wrapper.raw_desc.lookup.size());
            // process line
            char *next, *tmp;
            { // search for pid
                char *pid_end = begin + 32;
                if (end < pid_end) pid_end = end;
                next = find_next(begin, pid_end, '\t');
                if (next == pid_end) { // pid不存在
                    pid = -1;
                    next = begin;
                }
                else {
                    *next = 0;
                    pid = strtoll(begin, &tmp, 10);
                    CHECK(next == tmp);
                    ++next;
                }
            }
            while (next < end) {
                char *row_begin = next;
                char *row_end = find_next(row_begin, end, '|');
                // stop is either | or end
                next = find_next(row_begin, row_end, ',');
                CHECK(next < row_end);
                *next = 0;
                int fid = strtoll(row_begin, &tmp, 10);
                CHECK(tmp == next);
                ++next;
                // [next, row_end) : all the fields
                CHECK((fid >= 0) && (fid < rc_wrapper.raw_desc.formats.size()));
                auto const &fmt = rc_wrapper.raw_desc.formats[fid];
                if (fmt.use) {
                    auto &table = tables[fmt.tid];
                    table.emplace_back(next, row_end, fmt);
                    Row const &back = table.back();
                    if (pid < 0) {
                        pid = back.pid;
                    }
                    else {
                        CHECK(pid == back.pid);
                    }

                }
                next = row_end + 1;
            }
            //
            ctypes.resize(rc_wrapper.specs.size());
            //py::list ctor_params;
            //ctor_params.append(pid);
            int total_records = 0;
            for (int i = 0; i < ctypes.size(); ++i) {
                auto const &spec = rc_wrapper.specs[i];
                auto &ctype_table = ctypes[i];
                for (int version = 0; version < spec.versions.size(); ++version) {
                    RawTableDesc const *table_desc = spec.versions[version];
                    auto &table = tables[table_desc->tid];
                    for (auto &row: table) {
                        // extract records
                        Record &record = ctype_table.records.emplace_back();
                        ++total_records;
                        int64_t claim_no = row.claim_no;
                        record.fields.reserve(spec.fields.size());
                        for (auto const &field_spec: spec.fields) {
                            auto const &field_input_spec = field_spec.input_specs[version];
                            Field &field = record.fields.emplace_back();
                            if (field_input_spec.type == FIELD_ATOMIC) {
                                field.value = row.fields[field_input_spec.input_field->offset];
                            }
                            else if (field_input_spec.type == FIELD_MULTIPLE) {
                                load_column_groups(row, field_input_spec, &field);
                            }
                            else if (field_input_spec.type == FIELD_SUBTABLE) {
                                load_sub_table(tables, field_input_spec, claim_no, &field);
                            }
                            else CHECK(0);
                        }
                        CHECK(record.fields.size() == spec.fields.size());
                    }
                }
            }
            // extract claims
            claims.reserve(total_records);
            for (int tid = 0; tid < rc_wrapper.specs.size(); ++tid) {
                if (!rc_wrapper.specs[tid].is_claim) continue;
                bool is_snf_inp = rc_wrapper.specs[tid].is_snf_inp;
                for (auto const &r: ctypes[tid].records) {
                    Claim &claim = claims.emplace_back();
                    claim.spec = &rc_wrapper.specs[tid];
                    claim.is_snf_inp = is_snf_inp;
                    claim.fields.resize(rc_wrapper.claim_fields.size());
                    for (int i = 0; i < claim.fields.size(); ++i) {
                        // convert r to claim
                        int off = rc_wrapper.claim_fields[i].input_table_offsets[tid];
                        if (off < 0) {
                            claim.fields[i].is_none = true;
                        }
                        else {
                            auto const &i_f = r.fields[off];
                            CHECK(i_f.sub.empty());
                            claim.fields[i] = i_f.value;
                        }
                    }
                    if (claim.fields[CLAIM_CLAIM_NO].is_none) {
                        LOG(ERROR) << "CLAIM_NO is None, pid " << pid << " " << rc_wrapper.specs[tid].ctype;
                        claim.CLAIM_NO = -1;
                    }
                    else {
                        claim.CLAIM_NO = cast<int64_t>(claim.fields[CLAIM_CLAIM_NO]);

                    }
                    if (claim.fields[CLAIM_CLM_THRU_DT].is_none) {
                        LOG(ERROR) << "CLM_THRU_DT is None, pid " << pid << " " << rc_wrapper.specs[tid].ctype;
                        claim.CLM_THRU_DT = -1;
                    }
                    else {
                        claim.CLM_THRU_DT = cast<int64_t>(claim.fields[CLAIM_CLM_THRU_DT]);
                    }
                    if (claim.fields[CLAIM_CLM_ADMSN_DT].is_none) {
                        claim.CLM_ADMSN_DT = -1;
                    }
                    else {
                        claim.CLM_ADMSN_DT = cast<int64_t>(claim.fields[CLAIM_CLM_ADMSN_DT]);
                    }
                    // collect icd_codes
                    for (auto const &f: r.fields) {
                        if (f.value.is_none) {
                            CHECK(f.value.begin == f.value.end);
                        }
                        if ((f.value.icd_source >= 0) && (!f.value.is_none)) {
                            CHECK(f.value.end > f.value.begin) << (void *)f.value.begin << " " << (void *)f.value.end;
                            claim.icd_codes.emplace_back(f.value);
                        }
                        for (auto const &rr: f.sub) {
                            for (auto const &ff: rr.fields) {
                        if (ff.value.is_none) {
                            CHECK(ff.value.begin == ff.value.end);
                        }
                                if ((ff.value.icd_source >= 0) && (!ff.value.is_none)) {
                                    CHECK(ff.value.end > ff.value.begin) << (void *)ff.value.begin << " " << (void *)ff.value.end;
                                    claim.icd_codes.emplace_back(ff.value);
                                }
                            }
                        }
                    }
                }
            }
            std::sort(claims.begin(), claims.end());
            // extract DEMOS
            for (int tid = 0; tid < rc_wrapper.specs.size(); ++tid) {
                if (rc_wrapper.specs[tid].is_claim) continue;
                for (auto const &r: ctypes[tid].records) {
                    Demo &demo = demos.emplace_back();
                    demo.spec = &rc_wrapper.specs[tid];
                    demo.fields.resize(rc_wrapper.demo_fields.size());
                    for (int i = 0; i < demo.fields.size(); ++i) {
                        // convert r to claim
                        int off = rc_wrapper.demo_fields[i].input_table_offsets[tid];
                        CHECK(off >= 0);
                        if (off < 0) {
                            demo.fields[i].is_none = true;
                        }
                        else {
                            auto const &i_f = r.fields[off];
                            CHECK(i_f.sub.empty());
                            demo.fields[i] = i_f.value;
                        }
                    }
                    if (demo.fields[DEMO_REFERENCE_YEAR].is_none) {
                        CHECK(false);
                    }
                    else {
                        int ry = cast<int64_t>(demo.fields[DEMO_REFERENCE_YEAR]);
                        CHECK(ry >= 13 && ry <= 20) << "BAD REF YEAR " << ry;
                        demo.year = 2000 + ry;
                    }
                    if (demo.fields[DEMO_DATE_OF_DEATH].is_none) {
                        demo.DATE_OF_DEATH = -1;
                    }
                    else {
                        demo.DATE_OF_DEATH = cast<int64_t>(demo.fields[DEMO_DATE_OF_DEATH]);
                    }
                }
            }
            std::sort(demos.begin(), demos.end());
        }

        int size () const {
            return 0;
        }

        py::list get_claim_icd_codes () const {
            py::list r;
            for (auto const &c: claims) {
                py::list cc;
                for (auto const &code: c.icd_codes) {
                    cc.append(py::make_tuple(string(code.text), code.version, ICD_FIELD_PREFIXES[code.source]));
                }
                r.append(cc);
            }
            return r;
        }

        py::tuple generate_death_label (int cutoff) {
            static std::default_random_engine default_engine;
            int linear_cutoff = -1;
            if (cutoff > 0) {
                linear_cutoff = from_cms_date(cutoff);
            }
            int first_date = 20130101;
            int last_date = 20171231;
            int death_date = -1;
            bool dead = false;
            if (demos.size()) {
                first_date = demos.front().year * 10000 + 101;
                last_date = demos.back().year * 10000 + 1231;
                for (auto const &demo: demos) {
                    if (demo.DATE_OF_DEATH > 0) {
                        death_date = last_date = demo.DATE_OF_DEATH;
                        dead = true;
                    }
                }
            }
            int linear_first_date = from_cms_date(first_date);
            int linear_last_date = from_cms_date(last_date);
            /////
            int total_days = linear_last_date - linear_first_date + 1;
            CHECK(total_days > 0);
            int add;
            if (linear_cutoff > 0) {
                add = linear_cutoff - linear_first_date;
            }
            else {
                std::uniform_int_distribution<int> add_dist(0,total_days-1);
                add = add_dist(default_engine);
                // add >= 0; cutoff >= first_date
                // cutoff <= last_date
                linear_cutoff = linear_first_date + add;
            }
            int observe = total_days - add;
            cutoff = to_cms_date(linear_cutoff);

            int label = (dead && (observe < 365)) ? 1 : 0;
            return py::make_tuple(label, cutoff);
        }

        void merge_claims (bool absorb);
    };

    //static vector<int> const hist_ranges12720, 360, 180, 90, 30};
    static vector<int> const hist_ranges{720, 360, 180, 90, 30};
#if 1
    struct CaseHistogram {

        static int constexpr dim = 0;

        static void extract (Case const &cs, int cutoff, vector<float> *ft) {
        }
    };
#else
    struct CaseHistogram {

        static int constexpr dim = 5; //5;

        static void extract (Case const &cs, int cutoff, vector<float> *ft) {
            ft->resize(hist_ranges.size());
            std::fill(ft->begin(), ft->end(), 0);

            int linear_cutoff = from_cms_date(cutoff);
            for (auto const &claim: cs.claims) {
                int linear_dt = from_cms_date(claim.CLM_THRU_DT);
                int gap = linear_cutoff - linear_dt;
                CHEKC(gap > 0);
                auto &amt = claim.fields[CLAIM_CLM_TOT_CHRG_AMT];
                auto &amt2 = claim.fields[CLAIM_CLM_PMT_AMT];
                if (amt.is_none) continue;
                if (amt2.is_none) continue;
                for (int i = 0; i < hist_ranges.size(); ++i) {
                    if (gap > hist_ranges[i]) break;
                    ft->at(i) += amt2.f_value;// - amt2.f_value;
                }
            }
        }
    };
#endif


    bool try_merge_time_range (std::pair<int, int> a,
                    std::pair<int, int> b,
                    std::pair<int, int> *merged) {
        int a1 = a.first;
        int a2 = a.second;
        int b1 = b.first;
        int b2 = b.second;
        int left = std::max(a1, b1);
        int right = std::min(a2, b2);
        if (left - 1 <= right) {
            merged->first = std::min(a1, b1);
            merged->second = std::max(a2, b2);
            return true;
        }
        return false;
    }

    void merge_one (Claim *claim, Claim const &from) {
        // merge from to back
        claim->fields[CLAIM_CLM_PMT_AMT].add(from.fields[CLAIM_CLM_PMT_AMT]);
        claim->fields[CLAIM_CLM_TOT_CHRG_AMT].add(from.fields[CLAIM_CLM_TOT_CHRG_AMT]);
        claim->icd_codes.insert(claim->icd_codes.end(),
                from.icd_codes.begin(),
                from.icd_codes.end());
        claim->merge_count += from.merge_count;
    }

    void merge_one_type (vector<Claim> *merged,
                         vector<Claim *> &one_type,
                         vector<Claim *> &absorb,
                         vector<Claim *> *unabsorbed) {
        CHECK(unabsorbed->empty());
        int begin = 0;
        while (begin < one_type.size()) {
            auto range = one_type[begin]->time_range();
            int end = begin + 1;
            while (end < one_type.size()) {
                auto range2 = one_type[end]->time_range();
                if (!try_merge_time_range(range, range2, &range)) break;
                ++end;
            }
            // merge from begin to end
            {
                merged->push_back(*(one_type[begin]));
                auto &back = merged->back();
                back.CLM_THRU_DT = range.second;
                back.CLM_ADMSN_DT = range.first;
                for (int i = begin + 1; i < end; ++i) {
                    merge_one(&back, *(one_type[i]));
                }

                for (auto ptr: absorb) {
                    if ((ptr->CLM_THRU_DT >= range.first)
                            && (ptr->CLM_THRU_DT <= range.second)) {
                        merge_one(&back, *ptr);
                    }
                    else {
                        unabsorbed->push_back(ptr);
                    }
                }
            }
            begin = end;
        }
    }

    void Case::merge_claims (bool absorbe) {
        CHECK(!absorbe) << "No longer supported.";
        vector<vector<Claim *>> by_ctypes(CTYPE_TOTAL);
        for (auto &claim: claims) {
            int c = claim.spec->ctype_number;
            /*
            if (c == CTYPE_DME) {
                c = CTYPE_CAR;
            }
            */
            by_ctypes[c].push_back(&claim);
        }
        vector<Claim> merged;
        vector<Claim *> car, car2;
        if (absorbe) {
            by_ctypes[CTYPE_CAR].swap(car);
        }
        for (auto &one_type: by_ctypes) {
            merge_one_type(&merged, one_type, car, &car2);
            car.swap(car2);
            car2.clear();
        }
        if (absorbe) {
            by_ctypes[CTYPE_CAR].swap(car);
            CHECK(car.empty()); CHECK(car2.empty());
            merge_one_type(&merged, by_ctypes[CTYPE_CAR], car, &car2);
        }

        sort(merged.begin(), merged.end());
        claims.swap(merged);
    }

    Case *CaseLoaderWrapper::load (py::bytes bytes, bool copy) const {
        PyObject *ptr = bytes.ptr();
        char *buf = PyBytes_AsString(ptr);
        size_t sz = PyBytes_Size(ptr);
        return new Case(buf, buf + sz, *this, copy);
    }

    py::object CaseLoaderWrapper::get_norm_case (Case *c) const {
        py::list params;
        params.append(c->pid);
        for (int i = 0; i < c->ctypes.size(); ++i) {
            Table const &table = c->ctypes[i];
            auto const &spec = specs[i];
            py::list records;
            params.append(records);
            for (auto &r: table.records) {
                py::list fs;
                CHECK(r.fields.size() == spec.fields.size());
                for (int j = 0; j < spec.fields.size(); ++j) {
                    FieldSpecWrapper const &fspec = spec.fields[j];
                    if (fspec.subfields.empty()) {
                        fs.append(cast<py::object>(r.fields[j].value));
                    }
                    else {
                        // construct sub table
                        py::list sub;
                        fs.append(sub);
                        for (auto &sr: r.fields[j].sub) {
                            py::list sfs;
                            for (auto const &sf: sr.fields) {
                                sfs.append(cast<py::object>(sf.value));
                            }
                            sub.append(fspec.ctor(*sfs));
                        }
                    }
                }
                records.append(spec.ctor(*fs));
            }
        }
        return case_ctor(*params);
    }

    py::dict CaseLoaderWrapper::get_reduced_case (Case *c) const {
        vector<py::list> lists(claim_fields.size());
        for (auto const &claim: c->claims) {
            CHECK(claim.fields.size() == lists.size());
            for (int i = 0; i < lists.size(); ++i) {
                lists[i].append(cast<py::object>(claim.fields[i]));
            }
        }
        py::dict dict;
        for (int i = 0; i < lists.size(); ++i) {
            dict[py::str(claim_fields[i].name)] = lists[i];
        }
        return dict;
    }

    Matrix *CaseLoaderWrapper::get_extracted_case (Case *c, int cutoff) const {
        int n = 0;
        {
            int64_t last = -1;
            for (auto const &claim: c->claims) {
                CHECK(claim.CLM_THRU_DT >= last);
                last = claim.CLM_THRU_DT;
                if (claim.CLM_THRU_DT < cutoff) {
                    ++n;
                }
            }
        }
        CHECK(c->demos.size()) << "no demos pid " << c->pid;

        int claim_feature_size = feature_meta.size();

        if (n == 0) {
            Matrix *data = new Matrix(1, claim_feature_size);
            CHECK(data);
            Demo const *demo = &c->demos[0];
            int year = cutoff / 10000;
            CHECK(year >= 2013 && year <= 2200);
            for (auto const &d: c->demos) {
                if (d.year <= year) {
                    demo = &d;
                }
            }
            float *ft = &(*data)(0, 0);
            float *ft_end = ft + claim_feature_size;
            for (int j = 0; j < demo_fields.size(); ++j) {
                auto &desc = demo_fields[j];
                ft = desc.xtor->extract(demo->fields[j], ft, desc);
            }
            std::fill(ft, ft_end, numeric_limits<float>::quiet_NaN());
            return data;
        }

        Matrix *data = new Matrix(n, claim_feature_size);
        CHECK(data);
        for (int i = 0; i < n; ++i) {
            auto const &claim = c->claims[i];
            float *ft = &(*data)(i, 0);
            float *claim_ft_end = ft + claim_feature_size;

            // Search for corresponding demo
            int claim_year = claim.CLM_THRU_DT/10000;
            CHECK(claim_year >= 2013 && claim_year <= 2200);
            Demo const *demo = &c->demos[0];
            for (auto const &d: c->demos) {
                CHECK(d.year >= 2013 && d.year <= 2200);
                if (d.year <= claim_year) {
                    demo = &d;
                }
            }
            // add demo features
            for (int j = 0; j < demo_fields.size(); ++j) {
                auto &desc = demo_fields[j];
                ft = desc.xtor->extract(demo->fields[j], ft, desc);
            }
            // add claim field features
            for (int j = 0; j < claim_fields.size(); ++j) {
                auto &desc = claim_fields[j];
                ft = desc.xtor->extract(claim.fields[j], ft, desc);
            }
            // add claim-level features
            for (Xtor const *xtor: claim_xtors) {
                ft = xtor->extract(claim, *demo, ft, claim_fields, cutoff);
            }
            CHECK(ft == claim_ft_end);
        }
        return data;
        /*
        data = []
        for (auto const &claim: c->claims) {
            if (claim.CLM_THRU_DT >= time) break
                int date_diff1 = claim.CLM_THRU_DT - time;
                int date_diff2 = ADMSN_TIME - time;
                data.append([feature, date_diff1, date_diff2])
        }

        // data dims -> 2 x dims
        // x -> if isnan(x):    [-3000, 0]
        //      else       :    [x, 1]

        // x_mean = nanmean(data)
        // x_std = nanstd(data)
        // norm = np.nan_to_num((whole_train[i,:] - x_mean) / x_std)
        // whole_traim_normed

        // 最后7个claim flattern
        // 最后7个claim normed flattern
        // x_std 
        // % of NAN
        // val -> 第一个有效值
        // time_diff -> 第一个有效值和最后值之间的坐标距离
        */
    }


    void aggregate (Matrix *data, int begin, int end, float *ft, vector<FeatureMeta> const &meta) {
    }

    //int constexpr last_k = 12;
    int constexpr last_k = 12;
    
    py::list CaseLoaderWrapper::get_aggr_case_columns () const {
        py::list cols;
        // last
        for (int i = 0; i < last_k; ++i) {
            for (auto const &m: feature_meta) {
                cols.append(py::str(fmt::sprintf("%s-%d", m.name, last_k-i)));
            }
        }
        for (auto const &m: feature_meta) {
            cols.append(py::str(fmt::sprintf("%s-na", m.name)));
            cols.append(py::str(fmt::sprintf("%s-std", m.name)));
        }
        for (auto const &m: feature_meta) {
            cols.append(py::str(fmt::sprintf("%s-first", m.name)));
            cols.append(py::str(fmt::sprintf("%s-timediff", m.name)));
        }
        return cols;
    }

    Vector *CaseLoaderWrapper::get_aggr_case (Case *c, int cutoff) const {
        Matrix *claims = get_extracted_case(c, cutoff);
        int n = claims->rows();
        int cols = claims->cols();

#if 0
        int total_dim = CaseHistogram::dim;
        Vector *aggr = new Vector();
        *aggr = Vector::Constant(total_dim, numeric_limits<float>::quiet_NaN());

        float *ft = &(*aggr)(0,0);
#else

        int repl = last_k + 4;
        int total_dim = cols * repl + CaseHistogram::dim;
        Vector *aggr = new Vector();
        *aggr = Vector::Constant(total_dim, numeric_limits<float>::quiet_NaN());

        float *ft = &(*aggr)(0,0);

        int begin = std::max(n - last_k, 0);
        int copy_n = n - begin;
        int skip = last_k -  copy_n;

        // 0 1 2 3 4 5 6 7 8 9 10 [n==11]
        //       ^
        //       begin = 3 = n - 8
        // n >= 8:  begin = n - 8; copy_n = n - (n-8) = 8
        // n < 8:  begin = 0; copy_n = n - 0 = n, skip = 8 - n

        ft += cols * skip;
        for (int i = 0, in = begin; i < copy_n; ++i, ++in, ft += cols) {
            float const *ft_in = &(*claims)(in, 0);
            std::copy(ft_in, ft_in + cols, ft);
        }

        using namespace boost::accumulators;
        vector<accumulator_set<float, stats<tag::count, tag::mean, tag::variance> >> accs(cols);
        for (int i = 0; i < n; ++i) {
            float const *ft_in = &(*claims)(i, 0);
            for (int j = 0; j < cols; ++j) {
                float v = ft_in[j];
                if (std::isfinite(v)) {
                    accs[j](v);
                }
            }
        }
        for (int j = 0; j < cols; ++j) {
            int c = count(accs[j]);
            ft[j * 2] = 1.0 * (n - c) / n;
            if (c >= 1) {
                ft[j * 2 + 1] = std::sqrt(variance(accs[j]));
            }
            else {
                ft[j * 2 + 1] = numeric_limits<float>::quiet_NaN();
            }
        }
        ft += 2 * cols;

          for (int j = 0; j < cols; ++j) {
              float v = numeric_limits<float>::quiet_NaN();
              float timediff = v;
              for (int i = 0; i < n; ++i) {
                  float a = (*claims)(i, j);
                  if (std::isfinite(a)) {
                      v = a;
                      timediff = n - i;
                      break;
                  }
              }
              ft[j*2] = v;
              ft[j*2+1] = timediff;
          }

        ft += 2 * cols;
#endif

        vector<float> hist;
        CaseHistogram::extract (*c, cutoff, &hist);
        for (auto &v: hist) {
            ft[0] = v;
            ft += 1;
        }

        CHECK(ft == &(*aggr)(0,0) + total_dim);

    
        delete claims;

        return aggr;
    }


    enum CLAIM_FIELDS {
        TOTAL_CLAIM_FIELDS
    };

    class FileLoader: public CaseLoaderWrapper {
        FILE *file;
        char buf[LINE_BUF_SIZE];
    public:
        FileLoader (string const &path, py::object raw_case_loader)
            : CaseLoaderWrapper(raw_case_loader) {
            file = fopen(path.c_str(), "r");
            CHECK(file);
        }

        ~FileLoader () {
            fclose(file);
        }

        Case *next () {
            char *r = fgets(buf, LINE_BUF_SIZE, file);
            if (!r) return nullptr;
            size_t sz = strlen(buf);
            CHECK(sz + 1 < LINE_BUF_SIZE);
            return new Case(buf, buf+sz, *this);
        }

    };

    class FileFilter {
        unordered_map<uint64_t, int> lookup;
    public:
        FileFilter (py::list pids) {
            for (auto const &pid: pids) {
                lookup[py::cast<int64_t>(pid)] = 1;
            }
            LOG(INFO) << "loaded filter of " << lookup.size() << " items.";
        }
        void filter (string const &from, string const &to) {
            vector<char> mem(LINE_BUF_SIZE);
            char *buf = &mem[0];
            FILE *in, *out;
            in = fopen(from.c_str(), "r");
            CHECK(in);
            out = fopen(to.c_str(), "w");
            CHECK(out);
            for (;;) {
                char *r = fgets(buf, LINE_BUF_SIZE, in);
                if (!r) break;
                size_t sz = strlen(buf);
                CHECK(sz + 1 < LINE_BUF_SIZE);
                char *end = buf + sz;
                char *tab = find_next(buf, end, '\t');
                CHECK(tab < end);
                int64_t pid = rangetoll(buf, tab);
                if (lookup.count(pid) > 0) {
                    fwrite(buf, 1, sz, out);
                }
            }
            fclose(out);
            fclose(in);
        }
    };

    py::dict CaseLoaderWrapper::bulk_load_features (py::list py_paths,
                            py::list py_pick) const {
        boost::timer::auto_cpu_timer t;
        unordered_map<int64_t, vector<int>> pick;
        vector<string> paths;
        for (auto const &p: py_paths) {
            paths.push_back(py::cast<string>(p));
        }
        for (auto const &p: py_pick) {
            py::list l = py::cast<py::list>(p);
            int64_t pid = py::cast<int64_t>(l[0]);
            int time = py::cast<int>(l[1]);
            pick[pid].push_back(time);
        }
        py::dict loaded;
        LOG(INFO) << "loading " << pick.size() << " examples from " << paths.size() << " files.";
        boost::progress_display progress(paths.size(), std::cerr);
#pragma omp parallel for schedule(dynamic)
        for (int i = 0; i < paths.size(); ++i) {
            vector<char> mem(LINE_BUF_SIZE);
            char *buf = &mem[0];
            FILE *file = fopen(paths[i].c_str(), "r");
            CHECK(file);
            for (;;) {
                char *r = fgets(buf, LINE_BUF_SIZE, file);
                if (!r) break;
                size_t sz = strlen(buf);
                CHECK(sz > 0);
                CHECK(sz + 1 < LINE_BUF_SIZE);
                char *begin = buf;
                char *end = begin + sz;
                char *tab = find_next(begin, end, '\t');
                CHECK(tab < end);
                int64_t pid = rangetoll(begin, tab);
                auto it = pick.find(pid);
                if (it == pick.end()) continue;
                Case cs(begin, end, *this);
                vector<pair<int, Matrix *>> fts;
                for (auto cutoff: it->second) {
                    fts.emplace_back(cutoff, get_extracted_case(&cs, cutoff));
                }
#pragma omp critical
                {
                    for (auto p: fts) {
                        loaded[py::make_tuple(py::int_(pid), py::int_(p.first))] = py::detail::eigen_encapsulate<py::detail::EigenProps<Matrix>, Matrix>(p.second);
                    }
                }
            }
            fclose(file);
#pragma omp critical
            ++progress;
        }
        LOG(INFO) << py::len(loaded) << " examples loaded.";
        return loaded;
    }

    py::dict CaseLoaderWrapper::bulk_load_aggr_features (py::list py_paths,
                            py::list py_pick) const {
        boost::timer::auto_cpu_timer t;
        unordered_map<int64_t, vector<int>> pick;
        vector<string> paths;
        for (auto const &p: py_paths) {
            paths.push_back(py::cast<string>(p));
        }
        for (auto const &p: py_pick) {
            py::list l = py::cast<py::list>(p);
            int64_t pid = py::cast<int64_t>(l[0]);
            int time = py::cast<int>(l[1]);
            pick[pid].push_back(time);
        }
        py::dict loaded;
        LOG(INFO) << "loading " << pick.size() << " examples from " << paths.size() << " files.";
        boost::progress_display progress(paths.size(), std::cerr);
        int64_t total_claims = 0;
        int64_t merged_claims = 0;
#pragma omp parallel for schedule(dynamic) reduction(+:total_claims) reduction(+:merged_claims)
        for (int i = 0; i < paths.size(); ++i) {
            vector<char> mem(LINE_BUF_SIZE);
            char *buf = &mem[0];
            FILE *file = fopen(paths[i].c_str(), "r");
            CHECK(file);
            for (;;) {
                char *r = fgets(buf, LINE_BUF_SIZE, file);
                if (!r) break;
                size_t sz = strlen(buf);
                CHECK(sz > 0);
                CHECK(sz + 1 < LINE_BUF_SIZE);
                char *begin = buf;
                char *end = begin + sz;
                char *tab = find_next(begin, end, '\t');
                CHECK(tab < end);
                int64_t pid = rangetoll(begin, tab);
                auto it = pick.find(pid);
                if (it == pick.end()) continue;
                Case cs(begin, end, *this);
                total_claims += cs.claims.size();
                //cs.merge_claims(false);
                merged_claims += cs.claims.size();
                vector<pair<int, Vector *>> fts;
                for (auto cutoff: it->second) {
                    fts.emplace_back(cutoff, get_aggr_case(&cs, cutoff));
                }
#pragma omp critical
                {
                    for (auto p: fts) {
                        loaded[py::make_tuple(py::int_(pid), py::int_(p.first))] = py::detail::eigen_encapsulate<py::detail::EigenProps<Vector>, Vector>(p.second);
                    }
                }
            }
            fclose(file);
#pragma omp critical
            ++progress;
        }
        LOG(INFO) << py::len(loaded) << " examples loaded.";
        LOG(INFO) << "total claims: " << total_claims << " merged claims: " << merged_claims;
        return loaded;
    }


    py::array_t<double> grad_feature (py::array_t<double, py::array::c_style | py::array::forcecast> data, double nanval) {
          CHECK(data.ndim() == 2);
          int const rows = data.shape(0);
          int const cols = data.shape(1);
          vector<int> shape; shape.push_back(cols*2);
          py::array_t<double> ft(shape);
          auto mut = ft.mutable_unchecked<1>();
          double *ft_m = mut.mutable_data(0);
          for (int j = 0; j < cols; ++j) {
              double v = numeric_limits<double>::quiet_NaN();
              double timediff = v;
              for (int i = 0; i < rows; ++i) {
                  double a = data.at(i, j);
                  if (a != nanval) {
                      v = a;
                      timediff = rows - i;
                      break;
                  }
              }
              ft_m[j*2] = v;
              ft_m[j*2+1] = timediff;
          }
          return ft;
    }


    struct CaseDemosIterator {
        Case *c;
        int cutoff;
        CaseDemosIterator (Case *c_, int cutoff_ = -1): c(c_), cutoff(cutoff_) {
        }
    };

    struct CaseClaimsIterator {
        Case *c;
        int cutoff;
        CaseClaimsIterator (Case *c_, int cutoff_ = -1): c(c_), cutoff(cutoff_) {
        }
    };
        py::object Claim::getattr (string const &name) const {
            for (int i = 0; i < CLAIM_FIELD_DESCS.size(); ++i) {
                if (CLAIM_FIELD_DESCS[i].name == name) {
                    return cast<py::object>(fields[i]);
                }
            }
            return py::none();
        }
        py::object Demo::getattr (string const &name) const {
            for (int i = 0; i < DEMO_FIELD_DESCS.size(); ++i) {
                if (DEMO_FIELD_DESCS[i].name == name) {
                    return cast<py::object>(fields[i]);
                }
            }
            return py::none();
        }

        struct DemoMeta {
            int linear_year_begin;
            template <typename S>
            void serialize(S& s) {
                s.value4b(linear_year_begin);
            }

        };

        struct ClaimMeta {
            int linear_thru_dt;
            int linear_admsn_dt;
            bool is_snf_inp;
            int code_end;   // last index of code belonging to
                            // this claim + 1
            template <typename S>
            void serialize(S& s) {
                s.value4b(linear_thru_dt);
                s.value4b(linear_admsn_dt);
                s.value1b(is_snf_inp);
                s.value4b(code_end);
            }
        };

        struct Batch {
            Eigen::VectorXi labels;     // batch
            Matrix demos;      // batch * channels
            Matrix masks;      // batch * claims
            //vector<Matrix> claims;     // batch * claims * channels
            Eigen::Tensor<float, 3, Eigen::RowMajor> claims;
            Matrix codes;      // batch * codes
            //vector<Matrix> transfers;   // batch * claims * codes
            Eigen::Tensor<float, 3, Eigen::RowMajor> transfers;
            Eigen::VectorXi cutoffs;    // batch

            Batch (int batch, int demos_dim, int claims_dim, int f_claims, int f_codes) 
            : claims(batch, f_claims, claims_dim),
            transfers(batch, f_claims, f_codes) {
                labels = decltype(labels)::Constant(batch, 0);
                demos = decltype(demos)::Constant(batch, demos_dim, 0);
                masks = decltype(masks)::Constant(batch, f_claims, 1);
                claims.setConstant(0);
                codes = decltype(codes)::Constant(batch, f_codes, 0);
                transfers.setConstant(0);
                cutoffs = decltype(cutoffs)::Constant(batch, 0);
            }

            py::tuple py () {
                return py::make_tuple(labels, 
                       py::cast(demos),
                       py::cast(masks),
					   return_array(claims),
                       py::cast(codes),
					   return_array(transfers),
                       py::cast(cutoffs)
                       );
            }
        };

        struct Sample {
            int pid;
            int linear_death_date;
            int linear_first_date;
            int linear_last_date;
            vector<DemoMeta> demo_metas;
            vector<ClaimMeta> claim_metas;
            vector<int> snf_inp_indices;
            Matrix demos;   //at least one row
            Matrix claims;
            Vector icd_codes;

            template <typename S>
            void serialize(S& s) {
                int32_t SIGNATURE = 0xdeadbeef;
                int32_t signature = SIGNATURE;
                s.value4b(signature);
                CHECK(signature == SIGNATURE);
                s.value4b(pid);
                s.value4b(linear_death_date);
                s.value4b(linear_first_date);
                s.value4b(linear_last_date);
                s.container(demo_metas, 100000);
                s.container(claim_metas, 100000);
                s.container4b(snf_inp_indices, 1000000);
                serialize_matrix(s, demos);
                serialize_matrix(s, claims);
                serialize_vector(s, icd_codes);
            };




            py::tuple generate (int linear_cutoff, std::default_random_engine &engine, int task) {
                bool dead;
                int last_date;
                if (linear_death_date > 0) {
                    dead = true;
                    last_date = linear_death_date;
                }
                else {
                    dead = false;
                    last_date = linear_last_date;
                }
                int total_days = last_date - linear_first_date + 1;
                CHECK(total_days > 0);
                int add;
                if (linear_cutoff > 0) {
                    add = linear_cutoff - linear_first_date;
                }
                else {
                    std::uniform_int_distribution<int> add_dist(0,total_days-1);
                    add = add_dist(engine);
                    // add >= 0; cutoff >= first_date
                    // cutoff <= last_date
                    linear_cutoff = linear_first_date + add;

                    if (task == TASK_COMBINATION) {
                        if (claim_metas.size() > 0) {
                            std::uniform_int_distribution<int> coin(0, 1);
                            if ((snf_inp_indices.size() > 0) &&  (coin(engine) == 1)) {
                                // generate positive example
                                int idx = std::uniform_int_distribution<int>(0, snf_inp_indices.size()- 1)(engine);    // claim_metas might be empty
                                auto const &clm = claim_metas[snf_inp_indices[idx]];
                                CHECK(clm.linear_admsn_dt > 0);
                                linear_cutoff = clm.linear_admsn_dt - std::uniform_int_distribution<int>(0, 29)(engine);
                            }
                            else {
                                // generate negative example
                                if (coin(engine) == 1) {
                                    int idx = std::uniform_int_distribution<int>(0, claim_metas.size()- 1)(engine);    // claim_metas might be empty
                                    auto const &clm = claim_metas[idx];
                                    linear_cutoff = clm.linear_thru_dt + 1;

                                }
                            }
                        }
                        if (linear_cutoff < linear_first_date) {
                            linear_cutoff = linear_first_date;
                        }
                        add = linear_cutoff - linear_first_date;
                    }
                }
                int observe = total_days - add;

                //if (gs_label >= 0) CHECK(label == gs_label);

                int d = 0;
                for (int i = 0; i < demo_metas.size(); ++i) {
                    if (demo_metas[i].linear_year_begin <= linear_cutoff) {
                        d = i;
                    }
                }

                Vector sub_demo = demos.row(d);

                CHECK(claim_metas.size() >= 1);

                int n = 1;
                while (n < claim_metas.size()) {
                    if (claim_metas[n].linear_thru_dt >= linear_cutoff) break;
                    ++n;
                }

                int label = 0;
                if (task == TASK_COMBINATION) {
                    int fdate1 = linear_cutoff;
                    int fdate30 = fdate1 + 29;

                    for (int xxx = n; xxx < claim_metas.size(); ++xxx) {
                        auto const &clm = claim_metas[xxx];
                        if (clm.is_snf_inp) {
                            if (clm.linear_admsn_dt >= fdate1 &&
                                clm.linear_admsn_dt <= fdate30) {
                                label = 1;
                                break;
                            }
                        }
                    }
                }
                else if (task == TASK_MORTALITY) {
                    label = (dead && (observe < 365)) ? 1 : 0;
                }
                else CHECK(0) << "Unknown task " << task;


                Matrix sub_claims = claims.block(0, 0, n, claims.cols());

                int n_codes = 0;
                if (n > 0) {
                    n_codes = claim_metas[n-1].code_end;
                }
                Vector sub_codes = icd_codes.head(n_codes);

                Matrix transfer = Matrix::Constant(n, n_codes, 0);

                int c_off = 0;
                for (int i = 0; i < n; ++i) {
                    for (; c_off < claim_metas[i].code_end; ++c_off) {
                        transfer(i, c_off) = 1.0;
                    }
                }
                CHECK(c_off == n_codes);

                return py::make_tuple(label, 
                       py::cast(sub_demo),
                       py::cast(sub_claims),
                       py::cast(sub_codes),
                       py::cast(transfer),
                       to_cms_date(linear_cutoff)
                       //claims.rows()
                       );


            }

            py::tuple generate_fix (int linear_cutoff, std::default_random_engine &engine, int task, int fix_claims, int fix_codes) {
                bool dead;
                int last_date;
                if (linear_death_date > 0) {
                    dead = true;
                    last_date = linear_death_date;
                }
                else {
                    dead = false;
                    last_date = linear_last_date;
                }
                int total_days = last_date - linear_first_date + 1;
                CHECK(total_days > 0);
                int add;
                if (linear_cutoff > 0) {
                    add = linear_cutoff - linear_first_date;
                }
                else {
                    std::uniform_int_distribution<int> add_dist(0,total_days-1);
                    add = add_dist(engine);
                    // add >= 0; cutoff >= first_date
                    // cutoff <= last_date
                    linear_cutoff = linear_first_date + add;

                    if (task == TASK_COMBINATION) {
                        if (claim_metas.size() > 0) {
                            std::uniform_int_distribution<int> coin(0, 1);
                            if ((snf_inp_indices.size() > 0) &&  (coin(engine) == 1)) {
                                // generate positive example
                                int idx = std::uniform_int_distribution<int>(0, snf_inp_indices.size()- 1)(engine);    // claim_metas might be empty
                                auto const &clm = claim_metas[snf_inp_indices[idx]];
                                CHECK(clm.linear_admsn_dt > 0);
                                linear_cutoff = clm.linear_admsn_dt - std::uniform_int_distribution<int>(0, 29)(engine);
                            }
                            else {
                                // generate negative example
                                if (coin(engine) == 1) {
                                    int idx = std::uniform_int_distribution<int>(0, claim_metas.size()- 1)(engine);    // claim_metas might be empty
                                    auto const &clm = claim_metas[idx];
                                    linear_cutoff = clm.linear_thru_dt + 1;

                                }
                            }
                        }
                        if (linear_cutoff < linear_first_date) {
                            linear_cutoff = linear_first_date;
                        }
                        add = linear_cutoff - linear_first_date;
                    }
                }
                int observe = total_days - add;

                //if (gs_label >= 0) CHECK(label == gs_label);

                int d = 0;
                for (int i = 0; i < demo_metas.size(); ++i) {
                    if (demo_metas[i].linear_year_begin <= linear_cutoff) {
                        d = i;
                    }
                }

                Vector sub_demo = demos.row(d);

                CHECK(claim_metas.size() >= 1);

                int n = 1;
                while (n < claim_metas.size()) {
                    if (claim_metas[n].linear_thru_dt >= linear_cutoff) break;
                    ++n;
                }

                int label = 0;
                if (task == TASK_COMBINATION) {
                    int fdate1 = linear_cutoff;
                    int fdate30 = fdate1 + 29;

                    for (int xxx = n; xxx < claim_metas.size(); ++xxx) {
                        auto const &clm = claim_metas[xxx];
                        if (clm.is_snf_inp) {
                            if (clm.linear_admsn_dt >= fdate1 &&
                                clm.linear_admsn_dt <= fdate30) {
                                label = 1;
                                break;
                            }
                        }
                    }
                }
                else if (task == TASK_MORTALITY) {
                    label = (dead && (observe < 365)) ? 1 : 0;
                }
                else CHECK(0) << "Unknown task " << task;

                // <------
    {
        CHECK(fix_claims > 0);
        CHECK(fix_codes > 0);
        Vector claims_mask = Vector::Constant(fix_claims, 1);
        Matrix sub_claims; // = Matrix::Constant(fix_claims, claims.cols(), 0);
        int claims_from_begin = 0;
        int claims_from_end = n;
        int claims_to_begin = 0;
        // claims_to_end == fix_claims
        if (n >= fix_claims) {
            // drop data
            claims_from_begin = claims_from_end - fix_claims;
            sub_claims = claims.block(claims_from_begin, 0, claims_from_end-claims_from_begin, claims.cols());
        }
        else {
            sub_claims = Matrix::Constant(fix_claims, claims.cols(), 0);
            claims_to_begin = fix_claims - n;
            sub_claims.block(claims_to_begin, 0, fix_claims-claims_to_begin, claims.cols()) = claims.block(claims_from_begin, 0, claims_from_end-claims_from_begin, claims.cols());
            claims_mask.head(claims_to_begin) = Vector::Constant(claims_to_begin, 0);
        }
        CHECK(fix_claims - claims_to_begin == claims_from_end - claims_from_begin);


        int code_from_begin = 0;
        int code_from_end = 0;
        if (n > 0) {
            code_from_end = claim_metas[claims_from_end-1].code_end;
            if (claims_from_begin > 0) {
                code_from_begin = claim_metas[claims_from_begin-1].code_end;
            }
        }
        int code_skip = 0;
        if (code_from_end - code_from_begin > fix_codes) {
            code_skip = code_from_end - code_from_begin - fix_codes; 
        }

        // code_from_begin  --- code_from_begin + code_skip --- code_from_end
        //                                  | ---- len <= fix_codes ----| 
        Vector sub_codes = Vector::Constant(fix_codes, 0);
        CHECK(code_from_end - code_from_begin - code_skip <= fix_codes);
        sub_codes.head(code_from_end - code_from_begin - code_skip)
                        = icd_codes.segment(code_from_begin + code_skip, code_from_end - (code_from_begin + code_skip));

        Matrix transfer = Matrix::Constant(fix_claims, fix_codes, 0);

        int c_off = code_from_begin;
        int code_shift = code_from_begin + code_skip;
        // claims:  claims_from_begin   -----> claims_to_begin
        // codes:   code_from_begin + code_skip  -----> 0
        for (int i = claims_from_begin, j = claims_to_begin; i < claims_from_end; ++i, ++j) {
            CHECK(j < fix_claims);
            for (; c_off < claim_metas[i].code_end; ++c_off) {
                int cc = c_off - code_shift;
                if (cc >= 0) {
                    transfer(j, cc) = 1.0;
                }
            }
        }
        CHECK(c_off == code_from_end); // << c_off << " " << code_from_end << " " << claims_from_begin << " " << claims_from_end << " " << code_shift << " " << claim_metas[0].code_end;

        return py::make_tuple(label, 
               py::cast(sub_demo),
               py::cast(claims_mask),
               py::cast(sub_claims),
               py::cast(sub_codes),
               py::cast(transfer),
               to_cms_date(linear_cutoff)
               //claims.rows()
               );
    }
                CHECK(0);


            }

            void generate_batch (int linear_cutoff, std::default_random_engine &engine, int task, int fix_claims, int fix_codes, Batch *batch, int bo) {
                bool dead;
                int last_date;
                if (linear_death_date > 0) {
                    dead = true;
                    last_date = linear_death_date;
                }
                else {
                    dead = false;
                    last_date = linear_last_date;
                }
                int total_days = last_date - linear_first_date + 1;
                CHECK(total_days > 0);
                int add;
                if (linear_cutoff > 0) {
                    add = linear_cutoff - linear_first_date;
                }
                else {
                    std::uniform_int_distribution<int> add_dist(0,total_days-1);
                    add = add_dist(engine);
                    // add >= 0; cutoff >= first_date
                    // cutoff <= last_date
                    linear_cutoff = linear_first_date + add;

                    if (task == TASK_COMBINATION) {
                        if (claim_metas.size() > 0) {
                            std::uniform_int_distribution<int> coin(0, 1);
                            if ((snf_inp_indices.size() > 0) &&  (coin(engine) == 1)) {
                                // generate positive example
                                int idx = std::uniform_int_distribution<int>(0, snf_inp_indices.size()- 1)(engine);    // claim_metas might be empty
                                auto const &clm = claim_metas[snf_inp_indices[idx]];
                                CHECK(clm.linear_admsn_dt > 0);
                                linear_cutoff = clm.linear_admsn_dt - std::uniform_int_distribution<int>(0, 29)(engine);
                            }
                            else {
                                // generate negative example
                                if (coin(engine) == 1) {
                                    int idx = std::uniform_int_distribution<int>(0, claim_metas.size()- 1)(engine);    // claim_metas might be empty
                                    auto const &clm = claim_metas[idx];
                                    linear_cutoff = clm.linear_thru_dt + 1;

                                }
                            }
                        }
                        if (linear_cutoff < linear_first_date) {
                            linear_cutoff = linear_first_date;
                        }
                        add = linear_cutoff - linear_first_date;
                    }
                }
                int observe = total_days - add;

                //if (gs_label >= 0) CHECK(label == gs_label);

                int d = 0;
                for (int i = 0; i < demo_metas.size(); ++i) {
                    if (demo_metas[i].linear_year_begin <= linear_cutoff) {
                        d = i;
                    }
                }

                batch->demos.block(bo, 0, 1, demos.cols()) = demos.block(d, 0, 1, demos.cols());

                CHECK(claim_metas.size() >= 1);

                int n = 1;
                while (n < claim_metas.size()) {
                    if (claim_metas[n].linear_thru_dt >= linear_cutoff) break;
                    ++n;
                }

                int label = 0;
                if (task == TASK_COMBINATION) {
                    int fdate1 = linear_cutoff;
                    int fdate30 = fdate1 + 29;

                    for (int xxx = n; xxx < claim_metas.size(); ++xxx) {
                        auto const &clm = claim_metas[xxx];
                        if (clm.is_snf_inp) {
                            if (clm.linear_admsn_dt >= fdate1 &&
                                clm.linear_admsn_dt <= fdate30) {
                                label = 1;
                                break;
                            }
                        }
                    }
                }
                else if (task == TASK_MORTALITY) {
                    label = (dead && (observe < 365)) ? 1 : 0;
                }
                else CHECK(0) << "Unknown task " << task;

                // <------
    {
        batch->labels[bo] = label;
        CHECK(fix_claims > 0);
        CHECK(fix_codes > 0);
        int claims_from_begin = 0;
        int claims_from_end = n;
        int claims_to_begin = 0;
        // claims_to_end == fix_claims
        if (n >= fix_claims) {
            // drop data
            claims_from_begin = claims_from_end - fix_claims;
            size_t xxx = (claims_from_end-claims_from_begin) * claims.cols();
            float const *data = claims.block(claims_from_begin, 0, claims_from_end-claims_from_begin, claims.cols()).data();
            std::copy(data, data + xxx, batch->claims.data() + bo * xxx);
        }
        else {
            //batch->claims[bo] = Matrix::Constant(fix_claims, claims.cols(), 0);
            claims_to_begin = fix_claims - n;
            size_t xxx = n * claims.cols();
            float const *data = claims.block(claims_from_begin, 0, claims_from_end-claims_from_begin, claims.cols()).data();
            std::copy(data, data + xxx, batch->claims.data() + bo * fix_claims * claims.cols() + claims_to_begin * claims.cols());
            float *m_data = batch->masks.data() + bo * fix_claims;
            std::fill(m_data, m_data + claims_to_begin, 0);
        }
        CHECK(fix_claims - claims_to_begin == claims_from_end - claims_from_begin);


        int code_from_begin = 0;
        int code_from_end = 0;
        if (n > 0) {
            code_from_end = claim_metas[claims_from_end-1].code_end;
            if (claims_from_begin > 0) {
                code_from_begin = claim_metas[claims_from_begin-1].code_end;
            }
        }
        int code_skip = 0;
        if (code_from_end - code_from_begin > fix_codes) {
            code_skip = code_from_end - code_from_begin - fix_codes; 
        }

        // code_from_begin  --- code_from_begin + code_skip --- code_from_end
        //                                  | ---- len <= fix_codes ----| 
        //Vector sub_codes = Vector::Constant(fix_codes, 0);
        int code_to_copy = code_from_end - code_from_begin - code_skip;
        CHECK(code_to_copy <= fix_codes);
        Eigen::Map<Matrix> xxx(icd_codes.segment(code_from_begin + code_skip, code_to_copy).data(), 1, code_to_copy);
        batch->codes.block(bo, 0, 1, code_to_copy) = xxx;


        int c_off = code_from_begin;
        int code_shift = code_from_begin + code_skip;
        // claims:  claims_from_begin   -----> claims_to_begin
        // codes:   code_from_begin + code_skip  -----> 0
        for (int i = claims_from_begin, j = claims_to_begin; i < claims_from_end; ++i, ++j) {
            CHECK(j < fix_claims);
            for (; c_off < claim_metas[i].code_end; ++c_off) {
                int cc = c_off - code_shift;
                if (cc >= 0) {
                    batch->transfers(bo, j, cc) = 1.0;
                }
            }
        }
        CHECK(c_off == code_from_end); // << c_off << " " << code_from_end << " " << claims_from_begin << " " << claims_from_end << " " << code_shift << " " << claim_metas[0].code_end;
        batch->cutoffs[bo] = to_cms_date(linear_cutoff);
    }


            }


            Sample () {}
            Sample (Case *c, CaseLoaderWrapper const &wrapper) {
                pid = c->pid;

                linear_death_date = 0;
                linear_first_date = numeric_limits<int>::max();
                linear_last_date = 0;
                //CHECK(c->demos.size());
                demos.resize(std::max<int>(1, c->demos.size()), wrapper.demo_dimensions);
                if (c->demos.empty()) {
                    demos = Matrix::Constant(1, wrapper.demo_dimensions, numeric_limits<float>::quiet_NaN());
                    linear_first_date = linear_20130101;
                    linear_last_date = linear_20171231;
                }
                for (int i = 0; i < c->demos.size(); ++i) {
                    auto const &demo = c->demos[i];

                    float *ft = &demos(i, 0);
                    float *demo_ft_end = ft + demos.cols();

                    for (int j = 0; j < wrapper.demo_fields.size(); ++j) {
                        auto &desc = wrapper.demo_fields[j];
                        ft = desc.xtor->extract(demo.fields[j], ft, desc);
                    }
                    CHECK(ft == demo_ft_end);

                    int year_begin = from_cms_date(demo.year * 10000 + 101);
                    int year_end = from_cms_date(demo.year  * 10000 + 1231);

                    DemoMeta meta;
                    meta.linear_year_begin = year_begin;
                    demo_metas.push_back(meta);

                    linear_first_date = std::min(linear_first_date, year_begin);
                    linear_last_date = std::max(linear_last_date, year_end);
                    auto const &v = demo.fields[DEMO_DATE_OF_DEATH];
                    if (!v.is_none) {
                        linear_death_date = from_cms_date(v.i_value);
                    }

                }

                int claim_feature_size = wrapper.feature_meta.size();

                for (int j = 0; j < wrapper.claim_xtors.size(); ++j) {
                    Xtor const *xtor = wrapper.claim_xtors[j];
                    claim_feature_size -= xtor->size();
                }
                claim_feature_size += 1;

                claims.resize(c->claims.size() + 1, claim_feature_size);

                vector<int> all_codes;

                {   // add an artificial claim
                    ClaimMeta meta;
                    meta.linear_thru_dt = linear_first_date;
                    meta.linear_admsn_dt = -1;
                    meta.code_end = 0;
                    claim_metas.push_back(meta);

                    float *ft = &claims(0, 0);
                    float *claim_ft_end = ft + claims.cols();
                    std::fill(ft, claim_ft_end, numeric_limits<float>::quiet_NaN());
                    std::copy(&demos(0,0), &demos(0,0)+demos.cols(), ft);
                }

                for (int i = 0; i < c->claims.size(); ++i) {
                    auto const &claim = c->claims[i];
                    vector<int> codes;
                    for (ICDCode const &icd: claim.icd_codes) {
                        // TODO
                        if (icd.source == ICD_SOURCE_PRCDR) continue;
                        wrapper.icd9_tree.collect_codes(icd, &codes);
                    }
                    std::sort(codes.begin(), codes.end());
                    all_codes.insert(all_codes.end(), codes.begin(), std::unique(codes.begin(), codes.end()));
                    ClaimMeta meta;
                    meta.linear_thru_dt = from_cms_date(claim.CLM_THRU_DT);
                    meta.linear_admsn_dt = -1;
                    if (claim.CLM_ADMSN_DT > 0) {
                        meta.linear_admsn_dt = from_cms_date(claim.CLM_ADMSN_DT);
                    }
                    meta.is_snf_inp = claim.is_snf_inp;
                    meta.code_end = all_codes.size();

                    int last_linear_thru_dt;
                    last_linear_thru_dt = claim_metas.back().linear_thru_dt;
                    claim_metas.push_back(meta);


                    float *ft = &claims(i+1, 0);
                    float *claim_ft_end = ft + claims.cols();

                    // Search for corresponding demo
                    int claim_year = claim.CLM_THRU_DT/10000;
                    CHECK(claim_year >= 2013 && claim_year <= 2200);

                    float const *demo_ft = &demos(0, 0);

                    for (int xxx = 0; xxx < c->demos.size(); ++xxx) {
                        auto const &d = c->demos[xxx];
                        CHECK(d.year >= 2013 && d.year <= 2200);
                        if (d.year <= claim_year) {
                            demo_ft = &demos(xxx, 0);
                        }
                    }
                    // add demo features
                    std::copy(demo_ft, demo_ft + demos.cols(), ft);
                    ft += demos.cols();
                    /*
                    for (int j = 0; j < wrapper.demo_fields.size(); ++j) {
                        auto &desc = wrapper.demo_fields[j];
                        ft = desc.xtor->extract(demo->fields[j], ft, desc);
                    }
                    */
                    // add claim field features
                    for (int j = 0; j < wrapper.claim_fields.size(); ++j) {
                        auto &desc = wrapper.claim_fields[j];
                        ft = desc.xtor->extract(claim.fields[j], ft, desc);
                    }
                    // add claim-level features
                    /*
                    for (int j = 0; j < CLAIM_LEVEL_XTORS.size()-1; ++j) {
                        Xtor const *xtor = wrapper.claim_xtors[j];
                        ft = xtor->extract(claim, *demo, ft, wrapper.claim_fields, 0);
                    }
                    */
                    *ft = meta.linear_thru_dt - last_linear_thru_dt;
                    ++ft;
                    CHECK(ft == claim_ft_end) << (claim_ft_end - ft);
                }
                icd_codes.resize(all_codes.size());
                for (int i = 0; i < all_codes.size(); ++i) {
                    icd_codes(i) = all_codes[i];
                }
                for (int i = 0; i < claim_metas.size(); ++i) {
                    auto const &clm = claim_metas[i];
                    if (clm.is_snf_inp && (clm.linear_admsn_dt > 0)) {
                        snf_inp_indices.push_back(i);
                    }
                }
            }
        };

    class SampleLoader  {
    protected:

        std::default_random_engine engine;
        vector<Sample *> black;
        vector<Sample *> samples;
        unordered_map<int64_t, Sample *> lookup;
        int next_id;
        unordered_set<int64_t> blacklist;
        int task;

    public:
        int demo_dimensions;
        int claim_dimensions;
        int codebook_size;

        SampleLoader (string const &task_name, py::list py_paths, py::object loader, py::list py_black) //, bool train_, py::object py_gs)
            : engine(2021)
        {
            if (task_name == "combination") {
                task = TASK_COMBINATION;
            }
            else if (task_name == "mortality") {
                task = TASK_MORTALITY;
            }
            else CHECK(0) << "unknonw task name " << task_name;

            for (auto h: py_black) {
                blacklist.insert(py::cast<int64_t>(h));
            }
#if 0
            bool filter = false;
            unordered_map<int, pair<int, int>> gs;
            if (!py_gs.is_none()) {
                filter = true;
                for (auto h: py::cast<py::list>(py_gs)) {
                    py::list s = py::cast<py::list>(h);
                    int64_t pid = py::cast<int64_t>(s[0]);
                    int label = py::cast<int>(s[1]);
                    int linear_cutoff = from_cms_date(py::cast<int>(s[2]));
                    gs[pid] = std::make_pair(label, linear_cutoff);
                }
            }
#endif
            //FileLoader f(path, loader);
            vector<string> paths;
            for (auto const &p: py_paths) {
                paths.push_back(py::cast<string>(p));
            }
            CaseLoaderWrapper wrapper(loader);
            codebook_size = wrapper.icd9_tree.codebook_size;
            demo_dimensions = wrapper.demo_dimensions;
            claim_dimensions = wrapper.feature_meta.size() - codebook_size;
            LOG(INFO) << "loading " << paths.size() << " files.";
            boost::progress_display progress(paths.size(), std::cerr);
#pragma omp parallel for schedule(dynamic)
            for (int i = 0; i < paths.size(); ++i) {
                vector<char> mem(LINE_BUF_SIZE);
                char *buf = &mem[0];
                FILE *file = fopen(paths[i].c_str(), "r");
                CHECK(file);
                for (;;) {
                    char *r = fgets(buf, LINE_BUF_SIZE, file);
                    if (!r) break;
                    size_t sz = strlen(buf);
                    CHECK(sz > 0);
                    CHECK(sz + 1 < LINE_BUF_SIZE);
                    char *begin = buf;
                    char *end = begin + sz;
                    Case cs(begin, end, wrapper);
                    Sample *s = new Sample(&cs, wrapper);
                    bool is_black = blacklist.count(cs.pid);
#pragma omp critical
                    {
                        if (is_black) {
                            black.push_back(s);
                        }
                        else {
                            samples.push_back(s);
                        }
                    }
                }
                fclose(file);
#pragma omp critical
                ++progress;
            }
            for (Sample *s: black) {
                lookup[s->pid] = s;
            }
            for (Sample *s: samples) {
                lookup[s->pid] = s;
            }
            LOG(INFO) << "SAMPLE SIZE " << samples.size();
            LOG(INFO) << "BLACK SIZE " << black.size();
            //CHECK(samples.size() > 0);
            next_id = samples.size();
        }

        ~SampleLoader () {
            for (Sample *s: black) delete s;
            for (Sample *s: samples) delete s;
            black.clear();
            samples.clear();
            lookup.clear();
        }

        int size () const {
            return samples.size();
        }

        py::tuple next (int fix_claims, int fix_codes) {
            if (next_id >= samples.size()) {
                CHECK(samples.size() > 0);
                std::random_shuffle(samples.begin(), samples.end());
                next_id = 0;
            }
            Sample *s = samples[next_id++];
            // generate samples
            if (fix_claims > 0 || fix_codes > 0) {
                return s->generate_fix(-1, engine, task, fix_claims, fix_codes);
            }
            else {
                return s->generate(-1, engine, task);
            }
        }

        py::object get (int64_t pid, int cutoff, int fix_claims, int fix_codes) {
            auto it = lookup.find(pid);
            if (it == lookup.end()) return py::none();
            Sample *s = it->second;
            int linear_cutoff = cutoff <= 0 ? cutoff : from_cms_date(cutoff);
            if (fix_claims > 0 || fix_codes > 0) {
                return s->generate_fix(linear_cutoff, engine, task, fix_claims, fix_codes);
            }
            else {
                return s->generate(linear_cutoff, engine, task);
            }
        }
    };

    class LargeSampleLoader: public SampleLoader  {
        vector<string> paths;
        CaseLoaderWrapper wrapper;

        int batch_size;
        int fix_claims;
        int fix_codes;

        moodycamel::BlockingReaderWriterCircularBuffer<Batch *> channel;
        std::thread thread;

        void loader_thread () {
            int next_file = paths.size();   // so shuffle the first time
            vector<Sample *> samples;         // sample stack always take from top
            for (;;) {
                if (samples.size() < batch_size) {
                    int old_samples_size = samples.size();
                    while (samples.size() < batch_size) {
                        //LOG(INFO) << "XXX " << samples.size() << " " << batch_size;
                        if (next_file >= paths.size()) {
                            CHECK(paths.size() > 0);
                            std::random_shuffle(paths.begin(), paths.end());
                            next_file = 0;
                        }
                        string const &path = paths[next_file++];
                        // load file
#if 0

                        vector<vector<char>> bufs;
                        {
                            vector<char> mem(LINE_BUF_SIZE);
                            char *buf = &mem[0];
                            FILE *file = fopen(path.c_str(), "r");
                            CHECK(file);
                            for (;;) {
                                char *r = fgets(buf, LINE_BUF_SIZE, file);
                                if (!r) break;
                                size_t sz = strlen(buf);
                                CHECK(sz > 0);
                                CHECK(sz + 1 < LINE_BUF_SIZE);
                                char *begin = buf;
                                char *end = begin + sz;
                                bufs.emplace_back(begin, end);
                            }
                            fclose(file);
                        }
#pragma omp parallel for
                        for (int i = 0; i < bufs.size(); ++i) {
                            auto &mem = bufs[i];
                            char *begin = &mem[0];
                            char *end = begin + bufs[i].size();
                            Case cs(begin, end, wrapper, false);
                            if (blacklist.count(cs.pid) == 0) {
                                Sample *s = new Sample(&cs, wrapper);
#pragma omp critical
                                samples.push_back(s);
                            }
                        }
#else
                        {
            std::ifstream is(path, std::ios::binary);
            bitsery::Deserializer<bitsery::InputStreamAdapter> ser{is};
            int32_t ss;
            ser.value4b(ss);
            for (int i = 0; i < ss; ++i) {
                Sample *ptr = new Sample;
                ser.object(*ptr);
                samples.push_back(ptr);
            }
                        }
#endif
                    }
                    random_shuffle(samples.begin() + old_samples_size, samples.end());
                    std::reverse(samples.begin(), samples.end());
                }
                // generate one batch
                Batch *batch = new Batch(batch_size, samples.back()->demos.cols(), samples.back()->claims.cols(), fix_claims, fix_codes);
                for (int i = 0; i < batch_size; ++i) {
                    Sample *s = samples.back();
                    samples.pop_back();
                    s->generate_batch(-1, engine, task, fix_claims, fix_codes, batch, i);
                    delete s;
                }
                //LOG(INFO) << "ZZZ ";
                channel.wait_enqueue(batch);
            }
        }

        
    public:

        LargeSampleLoader (string const &task_name, py::list py_paths, py::object loader, py::list py_black, int batch_size_, int fix_claims_, int fix_codes_) //, bool train_, py::object py_gs)
            : SampleLoader(task_name, py::list(), loader, py_black), wrapper(loader),
            batch_size(batch_size_),
            fix_claims(fix_claims_),
            fix_codes(fix_codes_),
            channel(8)

        {
            //FileLoader f(path, loader);
            for (auto const &p: py_paths) {
                paths.push_back(py::cast<string>(p));
            }
            thread = std::thread([this](){this->loader_thread();});
        }

        ~LargeSampleLoader () {
            //for (Sample *s: samples) delete s;
            //samples.clear();
        }

        py::tuple next () {
            Batch *batch;
            channel.wait_dequeue(batch);
            py::tuple v = batch->py();
            delete batch;
            return v;
        }
    };

    void preload (string const &path, string const &output_path, py::object loader) {
        CaseLoaderWrapper wrapper(loader);
        vector<vector<char>> bufs;
        {
            vector<char> mem(LINE_BUF_SIZE);
            char *buf = &mem[0];
            FILE *file = fopen(path.c_str(), "r");
            CHECK(file);
            for (;;) {
                char *r = fgets(buf, LINE_BUF_SIZE, file);
                if (!r) break;
                size_t sz = strlen(buf);
                CHECK(sz > 0);
                CHECK(sz + 1 < LINE_BUF_SIZE);
                char *begin = buf;
                char *end = begin + sz;
                bufs.emplace_back(begin, end);
            }
            fclose(file);
        }
        vector<Sample *> samples;
#pragma omp parallel for
        for (int i = 0; i < bufs.size(); ++i) {
            auto &mem = bufs[i];
            char *begin = &mem[0];
            char *end = begin + bufs[i].size();
            Case cs(begin, end, wrapper, false);
            cs.merge_claims(false);
            Sample *s = new Sample(&cs, wrapper);
#pragma omp critical
            samples.push_back(s);
        }

        {
            std::ofstream os(output_path, std::ios::binary);
            bitsery::Serializer<bitsery::OutputBufferedStreamAdapter> ser{os};
            int32_t s = samples.size();
            ser.value4b(s);
            for (auto *s: samples) {
                ser.object(*s);
            }
            ser.adapter().flush();
        }
        

        for (Sample *s: samples) delete s;
    }

}


PYBIND11_MODULE(cms_core, module)
{
    using namespace cms;
    CHECK(to_cms_date(from_cms_date(19910301)) == 19910301);
    module.doc() = "";
    module.def("grad_feature", &grad_feature);
    module.def("from_cms_date", &from_cms_date);
    module.def("to_cms_date", &to_cms_date);
    module.def("preload", &preload);

    py::class_<CaseLoaderWrapper>(module, "CoreLoader")
         .def(py::init<py::object>())
         .def("load", py::overload_cast<py::bytes>(&CaseLoaderWrapper::load, py::const_))
         .def("load", py::overload_cast<py::bytes, bool>(&CaseLoaderWrapper::load, py::const_))
         .def("get_norm_case", &CaseLoaderWrapper::get_norm_case)
         .def("get_reduced_case", &CaseLoaderWrapper::get_reduced_case)
         .def("get_extracted_case", &CaseLoaderWrapper::get_extracted_case)
         .def("get_aggr_case", &CaseLoaderWrapper::get_aggr_case)
         .def("get_aggr_case_columns", &CaseLoaderWrapper::get_aggr_case_columns)
         .def("bulk_load_features", &CaseLoaderWrapper::bulk_load_features)
         .def("bulk_load_aggr_features", &CaseLoaderWrapper::bulk_load_aggr_features)
         .def("feature_columns", &CaseLoaderWrapper::feature_columns)
         .def("aggr_feature_columns", &CaseLoaderWrapper::aggr_feature_columns)
         .def("demo_columns", &CaseLoaderWrapper::demo_columns)
         .def("claim_columns", &CaseLoaderWrapper::claim_columns)
         ;

    py::class_<FileLoader>(module, "FileLoader")
         .def(py::init<string const &, py::object>())
         .def("next", &FileLoader::next)
         .def("get_norm_case", &FileLoader::get_norm_case)
         .def("get_reduced_case", &FileLoader::get_reduced_case)
         .def("get_extracted_case", &FileLoader::get_extracted_case)
         .def("get_aggr_case", &CaseLoaderWrapper::get_aggr_case)
         ;

    py::class_<SampleLoader>(module, "SampleLoader")
        .def(py::init<string, py::list, py::object, py::list>())
        .def("next", &SampleLoader::next)
        .def("get", &SampleLoader::get)
        .def("size", &SampleLoader::size)
        .def_readonly("codebook_size", &SampleLoader::codebook_size)
        .def_readonly("demo_dimensions", &SampleLoader::demo_dimensions)
        .def_readonly("claim_dimensions", &SampleLoader::claim_dimensions)
        ;

    py::class_<LargeSampleLoader>(module, "LargeSampleLoader")
        .def(py::init<string, py::list, py::object, py::list, int, int, int>())
        .def_readonly("codebook_size", &LargeSampleLoader::codebook_size)
        .def_readonly("demo_dimensions", &LargeSampleLoader::demo_dimensions)
        .def_readonly("claim_dimensions", &LargeSampleLoader::claim_dimensions)
        .def("next", &LargeSampleLoader::next)
        ;

    py::class_<Demo>(module, "CoreDemo")
         .def("ctype", &Demo::ctype)
         .def("__getattr__", &Demo::getattr)
        /*
         .def_readonly("CLAIM_NO", &Claim::CLAIM_NO)
         .def_readonly("CLM_THRU_DT", &Claim::CLM_THRU_DT)
         */
         ;

    py::class_<Claim>(module, "CoreClaim")
         .def_readonly("CLAIM_NO", &Claim::CLAIM_NO)
         .def_readonly("CLM_THRU_DT", &Claim::CLM_THRU_DT)
         .def("ctype", &Claim::ctype)
         .def("__getattr__", &Claim::getattr)
         .def("get_icd_codes", &Claim::get_icd_codes)
         ;

    py::class_<CaseClaimsIterator>(module, "CoreCaseClaimsIterator")
         .def("__iter__", [](CaseClaimsIterator &s) {
                 if (s.cutoff < 0) {
                    return py::make_iterator(s.c->claims.begin(), s.c->claims.end()); 
                 }
                 int off = 0;
                 while ((off < s.c->claims.size()) && (s.c->claims[off].CLM_THRU_DT < s.cutoff)) ++off;
                return py::make_iterator(s.c->claims.begin(), s.c->claims.begin()+off); 

                 }, py::keep_alive<0, 1>());
         ;


    py::class_<CaseDemosIterator>(module, "CoreCaseDemosIterator")
         .def("__iter__", [](CaseDemosIterator &s) {
                 if (s.cutoff < 0) {
                    return py::make_iterator(s.c->demos.begin(), s.c->demos.end()); 
                 }
                 int off = 0;
                 int cutoff_year = s.cutoff / 10000;
                 while ((off < s.c->demos.size()) && (s.c->demos[off].year <= cutoff_year)) ++off;
                return py::make_iterator(s.c->demos.begin(), s.c->demos.begin() + off); 
                 }, py::keep_alive<0, 1>() /* Essential: keep object alive while iterator exists */)
         ;

    py::class_<Case>(module, "CoreCase")
         .def_readonly("pid", &Case::pid)
         .def_readwrite("label", &Case::label)
         .def_readwrite("predict", &Case::predict)
         .def("cutoff", &Case::cutoff)
         .def("merge_claims", &Case::merge_claims)
         .def("drop_claims_on_thru_dt", &Case::drop_claims_on_thru_dt)
         .def("get_claim_icd_codes", &Case::get_claim_icd_codes)
         .def("generate_death_label", &Case::generate_death_label)
         .def("demos", [](Case &c) { return CaseDemosIterator(&c);})
         .def("claims", [](Case &c) { return CaseClaimsIterator(&c);})
         .def("demos_cutoff", [](Case &c, int co) { return CaseDemosIterator(&c, co);})
         .def("claims_cutoff", [](Case &c, int co) { return CaseClaimsIterator(&c, co);})
         ;

    py::class_<FileFilter>(module, "FileFilter")
         .def(py::init<py::list>())
         .def("filter", &FileFilter::filter)
         ;

    py::class_<Tree>(module, "Tree")
         .def(py::init<py::object>())
         .def("try_select", &Tree::try_select)
         ;
}

