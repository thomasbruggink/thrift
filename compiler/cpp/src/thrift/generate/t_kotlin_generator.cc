/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <cassert>
#include <ctime>

#include <sstream>
#include <string>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <vector>
#include <cctype>

#include <sys/stat.h>
#include <stdexcept>

#include "thrift/platform.h"
#include "thrift/generate/t_oop_generator.h"

using std::map;
using std::ostream;
using std::ostringstream;
using std::setfill;
using std::setw;
using std::string;
using std::stringstream;
using std::vector;

static const string endl = "\n"; // avoid ostream << std::endl flushes

/**
 * Kotlin code generator.
 *
 */
class t_kotlin_generator : public t_oop_generator {
public:
    t_kotlin_generator(t_program *program,
                       const std::map<std::string, std::string> &parsed_options,
                       const std::string &option_string)
            : t_oop_generator(program) {
      (void) option_string;
      std::map<std::string, std::string>::const_iterator iter;

      for (iter = parsed_options.begin(); iter != parsed_options.end(); ++iter) {
        throw "unknown option kotlin:" + iter->first;
      }

      out_dir_base_ = "gen-kotlin";
    }

    /**
     * Init and close methods
     */

    void init_generator() override;

    void close_generator() override;

    void generate_consts(std::vector<t_const *> consts) override;

    /**
     * Program-level generation functions
     */

    void generate_typedef(t_typedef *ttypedef) override;

    void generate_enum(t_enum *tenum) override;

    void generate_struct(t_struct *tstruct) override;

    void generate_union(t_struct *tunion);

    void generate_xception(t_struct *txception) override;

    void generate_service(t_service *tservice) override;

    void print_const_value(std::ostream &companion_out,
                           std::string name,
                           t_type *type,
                           t_const_value *value,
                           bool in_static,
                           bool defval = false);

    std::string render_const_value(std::ostream &out, t_type *type, t_const_value *value);

    void generate_private_variables(std::ostream &out, t_struct *tstruct);

    /**
     * Service-level generation functions
     */

    void generate_kotlin_struct(t_struct *tstruct, bool is_exception);

    void generate_kotlin_struct_definition(std::ostream &out,
                                           t_struct *tstruct,
                                           bool is_xception = false,
                                           bool in_class = false,
                                           bool is_result = false);

    void generate_kotlin_struct_parcelable(std::ostream &out, t_struct *tstruct);

    void generate_kotlin_struct_equality(std::ostream &out, t_struct *tstruct);

    void generate_kotlin_struct_compare_to(std::ostream &out, t_struct *tstruct);

    void generate_kotlin_struct_reader(std::ostream &out, t_struct *tstruct);

    void generate_kotlin_validator(std::ostream &out, t_struct *tstruct);

    void generate_kotlin_struct_result_writer(std::ostream &out, t_struct *tstruct);

    void generate_kotlin_struct_writer(std::ostream &out, t_struct *tstruct);

    void generate_kotlin_struct_tostring(std::ostream &out, t_struct *tstruct);

    void generate_kotlin_struct_clear(std::ostream &out, t_struct *tstruct);

    void generate_kotlin_struct_write_object(std::ostream &out, t_struct *tstruct);

    void generate_kotlin_struct_read_object(std::ostream &out, t_struct *tstruct);

    void generate_kotlin_meta_data_map(std::ostream &out, t_struct *tstruct);

    void generate_field_value_meta_data(std::ostream &out, t_type *type);

    std::string get_kotlin_type_string(t_type *type);

    void generate_kotlin_struct_field_by_id(ostream &out, t_struct *tstruct);

    void generate_reflection_setters(std::ostringstream &out,
                                     t_type *type,
                                     const std::string &field_name,
                                     const std::string &cap_name);

    void generate_reflection_getters(std::ostringstream &out,
                                     t_type *type,
                                     const std::string &field_name,
                                     const std::string &cap_name);

    void generate_generic_field_getters_setters(std::ostream &out, t_struct *tstruct);

    void generate_generic_isset_method(std::ostream &out, t_struct *tstruct);

    void generate_kotlin_bean_boilerplate(std::ostream &out, t_struct *tstruct);

    void generate_function_helpers(t_function *tfunction);

    std::string as_camel_case(std::string name, bool ucfirst = true);

    std::string get_rpc_method_name(std::string name);

    std::string get_cap_name(std::string name);

    std::string generate_isset_check(t_field *field);

    std::string generate_isset_check(std::string field);

    void generate_isset_set(ostream &out, t_field *field, std::string prefix);

    std::string isset_field_id(t_field *field);

    void generate_service_interface(t_service *tservice);

    void generate_service_async_interface(t_service *tservice);

    void generate_service_helpers(t_service *tservice);

    void generate_service_client(t_service *tservice);

    void generate_service_async_client(t_service *tservice);

    void generate_service_server(t_service *tservice);

    void generate_service_async_server(t_service *tservice);

    void generate_process_function(t_service *tservice, t_function *tfunction, bool async);

    void generate_kotlin_union(t_struct *tstruct);

    void generate_constructor(ostream &out, t_struct *tstruct);

    void generate_union_constructor(ostream& out, t_struct* tstruct);

    void generate_union_getters_and_setters(ostream &out, t_struct *tstruct);

    void generate_union_is_set_methods(ostream &out, t_struct *tstruct);

    void generate_union_abstract_methods(ostream &out, t_struct *tstruct);

    void generate_check_type(ostream &out, t_struct *tstruct);

    void generate_standard_scheme_read_value(ostream &out, t_struct *tstruct);

    void generate_standard_scheme_write_value(ostream &out, t_struct *tstruct);

    void generate_tuple_scheme_read_value(ostream &out, t_struct *tstruct);

    void generate_tuple_scheme_write_value(ostream &out, t_struct *tstruct);

    void generate_get_field_desc(ostream &out, t_struct *tstruct);

    void generate_get_struct_desc(ostream &out, t_struct *tstruct);

    void generate_get_field_name(ostream &out, t_struct *tstruct);

    void generate_union_comparisons(ostream &out, t_struct *tstruct);

    void generate_union_hashcode(ostream &out, t_struct *tstruct);

    void generate_scheme_map(ostream &out, t_struct *tstruct);

    void generate_standard_writer(ostream &out, t_struct *tstruct, bool is_result);

    void generate_standard_reader(ostream &out, t_struct *tstruct);

    void generate_kotlin_struct_standard_scheme(ostream &out, t_struct *tstruct, bool is_result);

    void generate_kotlin_struct_tuple_scheme(ostream &out, t_struct *tstruct);

    void generate_kotlin_struct_tuple_reader(ostream &out, t_struct *tstruct);

    void generate_kotlin_struct_tuple_writer(ostream &out, t_struct *tstruct);

    void generate_kotlin_scheme_lookup(ostream &out);

    void generate_java_generated_annotation(ostream &out);

    /**
     * Serialization constructs
     */

    void generate_deserialize_field(std::ostream &out,
                                    t_field *tfield,
                                    const std::string &prefix = "",
                                    bool has_metadata = true);

    void generate_deserialize_struct(std::ostream &out, t_struct *tstruct, const std::string &prefix = "");

    void generate_deserialize_container(std::ostream &out,
                                        t_type *ttype,
                                        const std::string &prefix = "",
                                        bool has_metadata = true);

    void generate_deserialize_set_element(std::ostream &out,
                                          t_set *tset,
                                          const std::string &prefix = "",
                                          const std::string &obj = "",
                                          bool has_metadata = true);

    void generate_deserialize_map_element(std::ostream &out,
                                          t_map *tmap,
                                          const std::string &prefix = "",
                                          const std::string &obj = "",
                                          bool has_metadata = true);

    void generate_deserialize_list_element(std::ostream &out,
                                           t_list *tlist,
                                           const std::string &prefix = "",
                                           const std::string &obj = "",
                                           bool has_metadata = true);

    void generate_serialize_field(std::ostream &out,
                                  t_field *tfield,
                                  const std::string &prefix = "",
                                  bool has_metadata = true);

    void generate_serialize_struct(std::ostream &out, t_struct *tstruct, const std::string &prefix = "");

    void generate_serialize_container(std::ostream &out,
                                      t_type *ttype,
                                      const std::string &prefix = "",
                                      bool has_metadata = true);

    void generate_serialize_map_element(std::ostream &out,
                                        t_map *tmap,
                                        const std::string &iter,
                                        const std::string &map,
                                        bool has_metadata = true);

    void generate_serialize_set_element(std::ostream &out,
                                        t_set *tmap,
                                        std::string iter,
                                        bool has_metadata = true);

    void generate_serialize_list_element(std::ostream &out,
                                         t_list *tlist,
                                         std::string iter,
                                         bool has_metadata = true);

    void generate_deep_copy_container(std::ostream &out,
                                      const std::string &source_name_p1,
                                      const std::string &source_name_p2,
                                      const std::string &result_name,
                                      t_type *type);

    void generate_deep_copy_non_container(std::ostream &out,
                                          const std::string &source_name,
                                          const std::string &dest_name,
                                          t_type *type);

    enum isset_type {
        ISSET_NONE, ISSET_PRIMITIVE, ISSET_BITSET
    };

    isset_type needs_isset(t_struct *tstruct, std::string *outPrimitiveType = nullptr);

    /**
     * Helper rendering functions
     */

    std::string kotlin_package();

    static std::string kotlin_suppressions();

    static std::string kotlin_nullable_annotation();

    std::string type_name(t_type *ttype,
                          bool in_container = false,
                          bool in_init = false,
                          bool skip_generic = false,
                          bool force_namespace = false,
                          bool generic_generic = false);

    std::string base_type_name(t_base_type *tbase, bool in_container = false);

    std::string declare_field(t_field *tfield, bool comment = false, bool last = false);

    std::string function_signature(t_function* tfunction,
                                   bool suspend = false,
                                   bool has_override = false,
                                   string visibility = "");

    std::string argument_list(t_struct *tstruct, bool include_types = true);

    std::string type_to_enum(t_type *ttype);

    void generate_struct_desc(ostream &out, t_struct *tstruct);

    void generate_field_descs(ostream &out, t_struct *tstruct);

    void generate_field_name_constants(ostream &out, t_struct *tstruct);

    std::string make_valid_kotlin_filename(std::string const &fromName);

    std::string make_valid_kotlin_identifier(std::string const &fromName);

    bool type_can_be_null(t_type *ttype) {
      ttype = get_true_type(ttype);

      return ttype->is_container() || ttype->is_struct() || ttype->is_xception() || ttype->is_string()
             || ttype->is_enum();
    }

    bool is_deprecated(const std::map<std::string, std::string> &annotations) {
      return annotations.find("deprecated") != annotations.end();
    }

    bool is_enum_set(t_type *ttype) {
      ttype = get_true_type(ttype);
      if (ttype->is_set()) {
        t_set *tset = (t_set *) ttype;
        t_type *elem_type = get_true_type(tset->get_elem_type());
        return elem_type->is_enum();
      }
      return false;
    }

    bool is_enum_map(t_type *ttype) {
      ttype = get_true_type(ttype);
      if (ttype->is_map()) {
        t_map *tmap = (t_map *) ttype;
        t_type *key_type = get_true_type(tmap->get_key_type());
        return key_type->is_enum();
      }
      return false;
    }

    std::string inner_enum_type_name(t_type *ttype) {
      ttype = get_true_type(ttype);
      if (ttype->is_map()) {
        t_map *tmap = (t_map *) ttype;
        t_type *key_type = get_true_type(tmap->get_key_type());
        return type_name(key_type, true) + "::class.java";
      } else if (ttype->is_set()) {
        t_set *tset = (t_set *) ttype;
        t_type *elem_type = get_true_type(tset->get_elem_type());
        return type_name(elem_type, true) + "::class.java";
      }
      return "";
    }

    std::string constant_name(const std::string &name);

private:
    /**
     * File streams
     */

    std::string package_name_;
    ofstream_with_content_based_conditional_update f_service_;
    std::string package_dir_;

};

/**
 * Prepares for file generation by opening up the necessary file output
 * streams.
 *
 * @param tprogram The program to generate
 */
void t_kotlin_generator::init_generator() {
  // Make output directory
  MKDIR(get_out_dir().c_str());
  package_name_ = program_->get_namespace("kotlin");

  string dir = package_name_;
  string subdir = get_out_dir();
  string::size_type loc;
  while ((loc = dir.find(".")) != string::npos) {
    subdir = subdir + "/" + dir.substr(0, loc);
    MKDIR(subdir.c_str());
    dir = dir.substr(loc + 1);
  }
  if (dir.size() > 0) {
    subdir = subdir + "/" + dir;
    MKDIR(subdir.c_str());
  }

  package_dir_ = subdir;
}

/**
 * Packages the generated file
 *
 * @return String of the package, i.e. "package org.apache.thriftdemo;"
 */
string t_kotlin_generator::kotlin_package() {
  if (!package_name_.empty()) {
    return string("package ") + package_name_ + ";\n\n";
  }
  return "";
}

string t_kotlin_generator::kotlin_suppressions() {
  // return "@SuppressWarnings({\"cast\", \"rawtypes\", \"serial\", \"unchecked\", \"unused\"})\n";
  return "";
}

string t_kotlin_generator::kotlin_nullable_annotation() {
  return "?";
}

/**
 * Nothing in kotlin
 */
void t_kotlin_generator::close_generator() {
}

/**
 * Generates a typedef. This is not done in kotlin, since it does
 * not support arbitrary name replacements, and it'd be a wacky waste
 * of overhead to make wrapper classes.
 *
 * @param ttypedef The type definition
 */
void t_kotlin_generator::generate_typedef(t_typedef *ttypedef) {
  (void) ttypedef;
}

/**
 * Enums are a class with a set of static constants.
 *
 * @param tenum The enumeration
 */
void t_kotlin_generator::generate_enum(t_enum *tenum) {
  bool is_deprecated = this->is_deprecated(tenum->annotations_);
  // Make output file
  string f_enum_name = package_dir_ + "/" + make_valid_kotlin_filename(tenum->get_name()) + ".kt";
  ofstream_with_content_based_conditional_update f_enum;
  f_enum.open(f_enum_name);

  // Comment and package it
  f_enum << autogen_comment() << kotlin_package() << endl;

  generate_java_doc(f_enum, tenum);

  generate_java_generated_annotation(f_enum);

  if (is_deprecated) {
    indent(f_enum) << "@Deprecated" << endl;
  }
  indent(f_enum) << "public enum class " << tenum->get_name() << "(" << endl
                 << indent() << "  override val value: Int" << endl
                 << indent() << "): org.apache.thrift.TEnum ";
  scope_up(f_enum);

  vector<t_enum_value *> constants = tenum->get_constants();
  vector<t_enum_value *>::iterator c_iter;
  bool first = true;
  for (c_iter = constants.begin(); c_iter != constants.end(); ++c_iter) {
    int value = (*c_iter)->get_value();

    if (first) {
      first = false;
    } else {
      f_enum << "," << endl;
    }

    generate_java_doc(f_enum, *c_iter);
    if (this->is_deprecated((*c_iter)->annotations_)) {
      indent(f_enum) << "@Deprecated" << endl;
    }
    indent(f_enum) << (*c_iter)->get_name() << "(" << value << ")";
  }
  f_enum << ";" << endl << endl;

  // Companion object
  indent(f_enum) << "companion object {" << endl;
  indent_up();
  indent(f_enum) << "/**" << endl;
  indent(f_enum) << " * Find a the enum type by its integer value, as defined in the Thrift IDL."
                 << endl;
  indent(f_enum) << " * @return null if the value is not found." << endl;
  indent(f_enum) << " */" << endl;
  indent(f_enum) << "fun findByValue(value: Int): " << tenum->get_name() << kotlin_nullable_annotation()
                 << " {" << endl;
  indent_up();

  indent(f_enum) << "return when (value) {" << endl;
  indent_up();

  for (c_iter = constants.begin(); c_iter != constants.end(); ++c_iter) {
    int value = (*c_iter)->get_value();
    indent(f_enum) << value << " -> " << endl;
    indent(f_enum) << "  " << (*c_iter)->get_name() << endl;
  }

  indent(f_enum) << "else ->" << endl;
  indent(f_enum) << "  null" << endl;

  // End of switch
  scope_down(f_enum);

  // End fun
  scope_down(f_enum);

  // End companion object
  scope_down(f_enum);

  // End of enum
  scope_down(f_enum);

  f_enum.close();
}

/**
 * Generates a class that holds all the constants.
 */
void t_kotlin_generator::generate_consts(std::vector<t_const *> consts) {
  if (consts.empty()) {
    return;
  }

  string f_consts_name = package_dir_ + '/' + make_valid_kotlin_filename(program_name_)
                         + "Constants.kt";
  ofstream_with_content_based_conditional_update f_consts;
  f_consts.open(f_consts_name);

  // Print header
  f_consts << autogen_comment() << kotlin_package() << kotlin_suppressions();

  f_consts << "public class " << make_valid_kotlin_identifier(program_name_) << "Constants {" << endl
           << endl;
  indent_up();
  indent(f_consts) << "companion object {" << endl;
  indent_up();
  vector<t_const *>::iterator c_iter;
  for (c_iter = consts.begin(); c_iter != consts.end(); ++c_iter) {
    generate_java_doc(f_consts, (*c_iter));
    print_const_value(f_consts,
                      (*c_iter)->get_name(),
                      (*c_iter)->get_type(),
                      (*c_iter)->get_value(),
                      false);
  }
  scope_down(f_consts);
  scope_down(f_consts);
  f_consts.close();
}


/**
 * Prints the value of a constant with the given type. Note that type checking
 * is NOT performed in this function as it is always run beforehand using the
 * validate_types method in main.cc
 */
void t_kotlin_generator::print_const_value(std::ostream &out,
                                           string name,
                                           t_type *type,
                                           t_const_value *value,
                                           bool in_static,
                                           bool defval) {
  type = get_true_type(type);

  indent(out);
  if (!defval) {
    if (type->is_base_type()) {
      out << "const ";
    }
    out << "val ";
  }
  out << name;
  if(!defval) {
     out << ":" << type_name(type);
  }
  if (type->is_base_type()) {
    string v2 = render_const_value(out, type, value);
    out << " = " << v2 << endl << endl;
  } else if (type->is_enum()) {
    out << " = " << render_const_value(out, type, value) << endl << endl;
  } else if (type->is_struct() || type->is_xception()) {
    const vector<t_field *> &unsorted_fields = ((t_struct *) type)->get_members();
    vector<t_field *> fields = unsorted_fields;
    std::sort(fields.begin(), fields.end(), t_field::key_compare());
    vector<t_field *>::const_iterator f_iter;
    const map<t_const_value *, t_const_value *, t_const_value::value_compare> &val = value->get_map();
    map<t_const_value *, t_const_value *, t_const_value::value_compare>::const_iterator v_iter;
    out << " = " << type_name(type, false, true) << "()" << endl;
    if (!in_static) {
      indent(out) << "init {" << endl;
      indent_up();
    }
    for (v_iter = val.begin(); v_iter != val.end(); ++v_iter) {
      t_type *field_type = nullptr;
      for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
        if ((*f_iter)->get_name() == v_iter->first->get_string()) {
          field_type = (*f_iter)->get_type();
        }
      }
      if (field_type == nullptr) {
        throw "type error: " + type->get_name() + " has no field " + v_iter->first->get_string();
      }
      string val = render_const_value(out, field_type, v_iter->second);
      indent(out) << name << ".";
      std::string cap_name = get_cap_name(v_iter->first->get_string());
      out << "set" << cap_name << "(" << val << ")" << endl;
    }
    if (!in_static) {
      indent_down();
      indent(out) << "}" << endl;
    }
    out << endl;
  } else if (type->is_map()) {
    std::string constructor_args;
    if (is_enum_map(type)) {
      constructor_args = inner_enum_type_name(type);
    }
    out << " = " << type_name(type, false, true) << "(" << constructor_args << ")" << endl;
    if (!in_static) {
      indent(out) << "init {" << endl;
      indent_up();
    }
    t_type *ktype = ((t_map *) type)->get_key_type();
    t_type *vtype = ((t_map *) type)->get_val_type();
    const map<t_const_value *, t_const_value *, t_const_value::value_compare> &val = value->get_map();
    map<t_const_value *, t_const_value *, t_const_value::value_compare>::const_iterator v_iter;
    for (v_iter = val.begin(); v_iter != val.end(); ++v_iter) {
      string renderConstKey = render_const_value(out, ktype, v_iter->first);
      string renderConstValue = render_const_value(out, vtype, v_iter->second);
      indent(out) << name << ".put(" << renderConstKey << ", " << renderConstValue << ")" << endl;
    }
    if (!in_static) {
      indent_down();
      indent(out) << "}" << endl;
    }
    out << endl;
  } else if (type->is_list() || type->is_set()) {
    if (is_enum_set(type)) {
      out << " = " << type_name(type, false, true, true) << ".noneOf(" << inner_enum_type_name(type) << ")"
          << endl;
    } else {
      out << " = " << type_name(type, false, true) << "()" << endl;
    }
    if (!in_static) {
      indent(out) << "init {" << endl;
      indent_up();
    }
    t_type *etype;
    if (type->is_list()) {
      etype = ((t_list *) type)->get_elem_type();
    } else {
      etype = ((t_set *) type)->get_elem_type();
    }
    const vector<t_const_value *> &val = value->get_list();
    vector<t_const_value *>::const_iterator v_iter;
    for (v_iter = val.begin(); v_iter != val.end(); ++v_iter) {
      string renderConstValue = render_const_value(out, etype, *v_iter);
      indent(out) << name;
      if(defval) {
        out << "?";
      }
      out << ".add(" << renderConstValue << ")" << endl;
    }
    if (!in_static) {
      indent_down();
      indent(out) << "}" << endl;
    }
    out << endl;
  } else {
    throw "compiler error: no const of type " + type->get_name();
  }
}

string t_kotlin_generator::render_const_value(ostream &out, t_type *type, t_const_value *value) {
  type = get_true_type(type);
  std::ostringstream render;

  if (type->is_base_type()) {
    t_base_type::t_base tbase = ((t_base_type *) type)->get_base();
    switch (tbase) {
      case t_base_type::TYPE_STRING:
        if (((t_base_type *) type)->is_binary()) {
          render << "java.nio.ByteBuffer.wrap(\"" << get_escaped_string(value) << "\".getBytes())";
        } else {
          render << '"' << get_escaped_string(value) << '"';
        }
        break;
      case t_base_type::TYPE_BOOL:
        render << ((value->get_integer() > 0) ? "true" : "false");
        break;
      case t_base_type::TYPE_I8:
        render << value->get_integer();
        break;
      case t_base_type::TYPE_I16:
        render << value->get_integer();
        break;
      case t_base_type::TYPE_I32:
        render << value->get_integer();
        break;
      case t_base_type::TYPE_I64:
        render << value->get_integer() << "L";
        break;
      case t_base_type::TYPE_DOUBLE:
        if (value->get_type() == t_const_value::CV_INTEGER) {
          render << value->get_integer() << "d";
        } else {
          render << emit_double_as_string(value->get_double());
        }
        break;
      default:
        throw "compiler error: no const of base type " + t_base_type::t_base_name(tbase);
    }
  } else if (type->is_enum()) {
    std::string namespace_prefix = type->get_program()->get_namespace("kotlin");
    if (namespace_prefix.length() > 0) {
      namespace_prefix += ".";
    }
    render << namespace_prefix << value->get_identifier_with_parent();
  } else {
    string t = tmp("tmp");
    print_const_value(out, t, type, value, true);
    render << t;
  }

  return render.str();
}

/**
 * Generates a struct definition for a thrift data type. This will be a org.apache.thrift.TBase
 * implementor.
 *
 * @param tstruct The struct definition
 */
void t_kotlin_generator::generate_struct(t_struct *tstruct) {
  if (tstruct->is_union()) {
    generate_kotlin_union(tstruct);
  } else {
    generate_kotlin_struct(tstruct, false);
  }
}

/**
 * Exceptions are structs, but they inherit from Exception
 *
 * @param tstruct The struct definition
 */
void t_kotlin_generator::generate_xception(t_struct *txception) {
  generate_kotlin_struct(txception, true);
}

/**
 * Writes all members as private variables.
 *
 * @param out stream
 */
void t_kotlin_generator::generate_private_variables(std::ostream &out, t_struct *tstruct) {
  const vector<t_field *> &members = tstruct->get_members();
  vector<t_field *>::const_iterator m_iter;
  for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
    generate_java_doc(out, *m_iter);
    indent(out) << "private var " <<
                declare_field(*m_iter, true, m_iter + 1 == members.end()) << endl;
  }
}

/**
 * kotlin struct definition.
 *
 * @param tstruct The struct definition
 */
void t_kotlin_generator::generate_kotlin_struct(t_struct *tstruct, bool is_exception) {
  // Make output file
  string f_struct_name = package_dir_ + "/" + make_valid_kotlin_filename(tstruct->get_name())
                         + ".kt";
  ofstream_with_content_based_conditional_update f_struct;
  f_struct.open(f_struct_name.c_str());

  f_struct << autogen_comment() << kotlin_package() << kotlin_suppressions();

  generate_kotlin_struct_definition(f_struct, tstruct, is_exception);
  f_struct.close();
}

/**
 * kotlin union definition.
 *
 * @param tstruct The struct definition
 */
void t_kotlin_generator::generate_kotlin_union(t_struct *tstruct) {
  ostringstream companion_out;

  // Make output file
  string f_struct_name = package_dir_ + "/" + make_valid_kotlin_filename(tstruct->get_name())
                         + ".kt";
  ofstream_with_content_based_conditional_update f_struct;
  f_struct.open(f_struct_name.c_str());

  f_struct << autogen_comment() << kotlin_package() << kotlin_suppressions();

  generate_java_doc(f_struct, tstruct);

  bool is_final = (tstruct->annotations_.find("final") != tstruct->annotations_.end());
  bool is_deprecated = this->is_deprecated(tstruct->annotations_);

  generate_java_generated_annotation(f_struct);

  if (is_deprecated) {
    indent(f_struct) << "@Deprecated" << endl;
  }
  indent(f_struct) << "public " << (is_final ? "final " : "") << "class " << tstruct->get_name() << "()"
                   << " : org.apache.thrift.TUnion<" << tstruct->get_name() << ", "
                   << tstruct->get_name() << ".Fields>() ";
  scope_up(f_struct);

  // Copy constructor
  indent(f_struct) << "constructor(other: " << tstruct->get_name() << "): this() {" << endl;
  indent_up();
  indent(f_struct) << "fromDeepCopy(other)" << endl;
  scope_down(f_struct);

  // clone method, so that you can deep copy an object when you don't know its class.
  indent(f_struct) << "override fun deepCopy(): " << tstruct->get_name() << "{" << endl;
  indent(f_struct) << "  return " << tstruct->get_name() << "(this)" << endl;
  indent(f_struct) << "}" << endl << endl;

  companion_out << "companion object ";
  scope_up(companion_out);
  generate_struct_desc(companion_out, tstruct);
  generate_field_descs(companion_out, tstruct);
  generate_union_constructor(companion_out, tstruct);
  scope_down(companion_out);

  companion_out << endl;

  generate_field_name_constants(companion_out, tstruct);

  companion_out << endl;

  indent_down();
  generate_kotlin_meta_data_map(f_struct, tstruct);
  indent_up();

  f_struct << endl;

  generate_union_abstract_methods(f_struct, tstruct);

  f_struct << endl;

  generate_kotlin_struct_field_by_id(f_struct, tstruct);

  f_struct << endl;

  generate_union_getters_and_setters(f_struct, tstruct);

  f_struct << endl;

  generate_union_is_set_methods(f_struct, tstruct);

  f_struct << endl;

  generate_union_comparisons(f_struct, tstruct);

  f_struct << endl;

  generate_union_hashcode(f_struct, tstruct);

  f_struct << endl;

  generate_kotlin_struct_write_object(f_struct, tstruct);

  f_struct << endl;

  generate_kotlin_struct_read_object(f_struct, tstruct);

  f_struct << endl;

  indent(f_struct) << companion_out.str() << endl;

  scope_down(f_struct);

  f_struct.close();
}

void t_kotlin_generator::generate_constructor(ostream &out, t_struct *tstruct) {
  const vector<t_field *> &members = tstruct->get_members();
  vector<t_field *>::const_iterator m_iter;

  bool all_optional_members = true;

  if (!members.empty() && !all_optional_members) {
    // all fields full constructor for ds
    indent(out) << "constructor(" << endl;
    indent_up();
    bool first = true;
    for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
      if ((*m_iter)->get_req() != t_field::T_OPTIONAL) {
        if (!first) {
          out << "," << endl;
        }
        first = false;
        indent(out) << type_name((*m_iter)->get_type()) << " " << (*m_iter)->get_name();
      }
    }
    out << ")" << endl;
    indent_down();
    indent(out) << "{" << endl;
    indent_up();
    indent(out) << "this()" << endl;
    for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
      if ((*m_iter)->get_req() != t_field::T_OPTIONAL) {
        t_type *type = get_true_type((*m_iter)->get_type());
        if (type->is_binary()) {
          indent(out) << "this." << (*m_iter)->get_name()
                      << " = org.apache.thrift.TBaseHelper.copyBinary(" << (*m_iter)->get_name()
                      << ");" << endl;
        } else {
          indent(out) << "this." << (*m_iter)->get_name() << " = " << (*m_iter)->get_name() << endl;
        }
        generate_isset_set(out, (*m_iter), "");
      }
    }

    indent_down();
    indent(out) << "}" << endl << endl;
  }
}

void t_kotlin_generator::generate_union_constructor(ostream& out, t_struct* tstruct) {
  const vector<t_field*>& members = tstruct->get_members();
  vector<t_field*>::const_iterator m_iter;

  // generate "constructors" for each field
  for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
    t_type* type = (*m_iter)->get_type();
    indent(out) << "fun " << (*m_iter)->get_name() << "("
                << "value: " << type_name(type) << "): " << type_name(tstruct) << "{"
                << endl;
    indent(out) << "  val x:" << type_name(tstruct) << " = " << type_name(tstruct) << "()" << endl;
    indent(out) << "  x.set" << get_cap_name((*m_iter)->get_name()) << "(value)" << endl;
    indent(out) << "  return x" << endl;
    indent(out) << "}" << endl << endl;

    if (type->is_binary()) {
      indent(out) << "fun " << (*m_iter)->get_name() << "(value: ByteArray): " 
                  << type_name(tstruct) << " {" << endl;
      indent(out) << "  val x:"<< type_name(tstruct) << " = " << type_name(tstruct) << "()"
                  << endl;
      indent(out) << "  x.set" << get_cap_name((*m_iter)->get_name()) << "(java.nio.ByteBuffer.wrap(value.clone()))" << endl;
      indent(out) << "  return x" << endl;
      indent(out) << "}" << endl << endl;
    }
  }
}

void t_kotlin_generator::generate_union_getters_and_setters(ostream &out, t_struct *tstruct) {
  const vector<t_field *> &members = tstruct->get_members();
  vector<t_field *>::const_iterator m_iter;

  bool first = true;
  for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
    if (first) {
      first = false;
    } else {
      out << endl;
    }

    t_field *field = (*m_iter);
    t_type *type = field->get_type();
    std::string cap_name = get_cap_name(field->get_name());
    bool is_deprecated = this->is_deprecated(field->annotations_);

    generate_java_doc(out, field);
    if (type->is_binary()) {
      if (is_deprecated) {
        indent(out) << "@Deprecated" << endl;
      }
      indent(out) << "fun get" << cap_name << "(): ByteArray? {" << endl;
      indent(out) << "  val b: java.nio.ByteBuffer? = buffer" << get_cap_name("for") << cap_name << "()" << endl;
      indent(out) << "  return b?.array()" << endl;
      indent(out) << "}" << endl;

      out << endl;

      indent(out) << "fun buffer" << get_cap_name("for")
                  << get_cap_name(field->get_name()) << "(): java.nio.ByteBuffer? {" << endl;
      indent(out) << "  if (uSetField == Fields." << constant_name(field->get_name()) << ") {"
                  << endl;

      indent(out)
              << "    return org.apache.thrift.TBaseHelper.copyBinary(getFieldValue() as java.nio.ByteBuffer)"
              << endl;

      indent(out) << "  } else {" << endl;
      indent(out) << "    throw java.lang.RuntimeException(\"Cannot get field '" << field->get_name()
                  << "' because union is currently set to ${getFieldDesc(uSetField).name}\")"
                  << endl;
      indent(out) << "  }" << endl;
      indent(out) << "}" << endl;
    } else {
      if (is_deprecated) {
        indent(out) << "@Deprecated" << endl;
      }
      indent(out) << "fun get" << get_cap_name(field->get_name()) 
                  << "(): " << type_name(field->get_type()) << " {" << endl;
      indent(out) << "  if (uSetField == Fields." << constant_name(field->get_name()) << ") {"
                  << endl;
      indent(out) << "    return uFieldValue as " << type_name(field->get_type(), true)
                  << endl;
      indent(out) << "  } else {" << endl;
      indent(out) << "    throw java.lang.RuntimeException(\"Cannot get field '" << field->get_name()
                  << "' because union is currently set to ${getFieldDesc(uSetField).name})\")"
                  << endl;
      indent(out) << "  }" << endl;
      indent(out) << "}" << endl;
    }

    out << endl;

    generate_java_doc(out, field);
    if (type->is_binary()) {
      if (is_deprecated) {
        indent(out) << "@Deprecated" << endl;
      }
      indent(out) << "fun set" << get_cap_name(field->get_name()) << "(value: ByteArray) {"
                  << endl;
      indent(out) << "  set" << get_cap_name(field->get_name());

      indent(out) << "(java.nio.ByteBuffer.wrap(value.clone()));" << endl;

      indent(out) << "}" << endl;

      out << endl;
    }
    if (is_deprecated) {
      indent(out) << "@Deprecated" << endl;
    }
    indent(out) << "fun set" << get_cap_name(field->get_name()) << "("
                << "value: " << type_name(field->get_type()) << ") {" << endl;
    indent(out) << "  uSetField = Fields." << constant_name(field->get_name()) << endl;
    indent(out) << "  uFieldValue = value" << endl;
    indent(out) << "}" << endl;
  }
}

void t_kotlin_generator::generate_union_is_set_methods(ostream &out, t_struct *tstruct) {
  const vector<t_field *> &members = tstruct->get_members();
  vector<t_field *>::const_iterator m_iter;

  bool first = true;
  for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
    if (first) {
      first = false;
    } else {
      out << endl;
    }

    std::string field_name = (*m_iter)->get_name();

    indent(out) << "fun is" << get_cap_name("set") << get_cap_name(field_name) << "(): Boolean {"
                << endl;
    indent_up();
    indent(out) << "return uSetField == Fields." << constant_name(field_name) << endl;
    indent_down();
    indent(out) << "}" << endl << endl;
  }
}

void t_kotlin_generator::generate_union_abstract_methods(ostream &out, t_struct *tstruct) {
  generate_check_type(out, tstruct);
  out << endl;
  generate_standard_scheme_read_value(out, tstruct);
  out << endl;
  generate_standard_scheme_write_value(out, tstruct);
  out << endl;
  generate_tuple_scheme_read_value(out, tstruct);
  out << endl;
  generate_tuple_scheme_write_value(out, tstruct);
  out << endl;
  generate_get_field_desc(out, tstruct);
  out << endl;
  generate_get_struct_desc(out, tstruct);
  out << endl;
  indent(out) << "override fun enumForId(id: Short): Fields {" << endl;
  indent(out) << "  return Fields.findByThriftIdOrThrow(id)" << endl;
  indent(out) << "}" << endl;
}

void t_kotlin_generator::generate_check_type(ostream &out, t_struct *tstruct) {
  indent(out) << "@Throws(java.lang.ClassCastException::class)" << endl;
  indent(out)
          << "override fun checkType(setField: Fields, value: Any?) {"
          << endl;
  indent_up();

  indent(out) << "when (setField) {" << endl;
  indent_up();

  const vector<t_field *> &members = tstruct->get_members();
  vector<t_field *>::const_iterator m_iter;

  for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
    t_field *field = (*m_iter);

    indent(out) << "Fields." << constant_name(field->get_name()) << " -> {" << endl;
    indent(out) << "  if (value !is " << type_name(field->get_type(), true, false, true, false, true)
                << ") {" << endl;
    indent(out) << "    throw java.lang.ClassCastException(\"Was expecting value of type "
                << type_name(field->get_type(), true, false) << " for field '" << field->get_name()
                << "', but got ${value?.javaClass?.simpleName}\")" << endl;
    indent(out) << "  }" << endl;
    indent(out) << "}" << endl;
    // do the real check here
  }

  indent(out) << "else -> " << endl;
  indent(out) << "  throw java.lang.IllegalArgumentException(\"Unknown field id $setField\")" << endl;

  indent_down();
  indent(out) << "}" << endl;

  indent_down();
  indent(out) << "}" << endl;
}

void t_kotlin_generator::generate_standard_scheme_read_value(ostream &out, t_struct *tstruct) {
  indent(out) << "@Throws(org.apache.thrift.TException::class)" << endl;
  indent(out) << "override suspend fun standardSchemeReadValue( "
              << "iprot: org.apache.thrift.protocol.TProtocol, field: org.apache.thrift.protocol.TField): Any? {" << endl;

  indent_up();

  indent(out) << "val setField:Fields? = Fields.findByThriftId(field.id)" << endl;
  indent(out) << "if (setField != null) {" << endl;
  indent_up();
  indent(out) << "when (setField) {" << endl;
  indent_up();

  const vector<t_field *> &members = tstruct->get_members();
  vector<t_field *>::const_iterator m_iter;

  for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
    t_field *field = (*m_iter);
    const t_type *ttype = field->get_type()->get_true_type();

    indent(out) << "Fields." << constant_name(field->get_name()) << " -> {" << endl;
    indent_up();
    indent(out) << "if (field.type == " << constant_name(field->get_name()) << "_FIELD_DESC.type) {"
                << endl;
    indent_up();
    indent(out) << "var " << field->get_name() << ": "<< type_name(field->get_type(), true, false);
    if(ttype->is_enum()) {
      out << "?";
    }
    indent(out) << endl;
    generate_deserialize_field(out, field, "");
    indent(out) << "return " << field->get_name() << endl;
    indent_down();
    indent(out) << "} else {" << endl;
    indent(out) << "  org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type)" << endl;
    indent(out) << "  return null" << endl;
    indent(out) << "}" << endl;
    indent_down();
    indent(out) << "}" << endl;
  }

  indent(out) << "else ->" << endl;
  indent(out) << "  throw java.lang.IllegalStateException(\"setField wasn't null, but didn't match any "
                 "of the case statements!\");" << endl;

  indent_down();
  indent(out) << "}" << endl;

  indent_down();
  indent(out) << "} else {" << endl;
  indent_up();
  indent(out) << "org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type)" << endl;
  indent(out) << "return null" << endl;
  indent_down();
  indent(out) << "}" << endl;

  indent_down();
  indent(out) << "}" << endl;
}

void t_kotlin_generator::generate_standard_scheme_write_value(ostream &out, t_struct *tstruct) {
  indent(out) << "@Throws(org.apache.thrift.TException::class)" << endl;
  indent(out) << "override suspend fun standardSchemeWriteValue(oprot: org.apache.thrift.protocol.TProtocol) {" << endl;

  indent_up();

  indent(out) << "when (uSetField) {" << endl;
  indent_up();

  const vector<t_field *> &members = tstruct->get_members();
  vector<t_field *>::const_iterator m_iter;

  for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
    t_field *field = (*m_iter);

    indent(out) << "Fields." << constant_name(field->get_name()) << " -> {" << endl;
    indent_up();
    indent(out) << "val " << field->get_name() << ": " << type_name(field->get_type(), true, false) << " = "
                << "uFieldValue as " << type_name(field->get_type(), true, false) << endl;
    generate_serialize_field(out, field, "");
    indent(out) << "return" << endl;
    indent_down();
    indent(out) << "}" << endl;
  }

  indent(out) << "else ->" << endl;
  indent(out) << "  throw java.lang.IllegalStateException(\"Cannot write union with unknown field $uSetField\")" << endl;

  indent_down();
  indent(out) << "}" << endl;

  indent_down();

  indent(out) << "}" << endl;
}

void t_kotlin_generator::generate_tuple_scheme_read_value(ostream &out, t_struct *tstruct) {
  indent(out) << "override suspend fun tupleSchemeReadValue(iprot: org.apache.thrift.protocol.TProtocol"
              << ", fieldID:Short):Any? {" << endl;

  indent_up();

  indent(out) << "val setField:Fields? = Fields.findByThriftId(fieldID)" << endl;
  indent(out) << "if (setField != null) {" << endl;
  indent_up();
  indent(out) << "return when (setField) {" << endl;
  indent_up();

  const vector<t_field *> &members = tstruct->get_members();
  vector<t_field *>::const_iterator m_iter;

  for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
    t_field *field = (*m_iter);
    const t_type *ttype = field->get_type()->get_true_type();

    indent(out) << "Fields." << constant_name(field->get_name()) << " -> {" << endl;
    indent_up();
    indent(out) << "val " << field->get_name() << ":" << type_name(field->get_type(), true, false);
    if(ttype->is_enum()) {
      out << "?";
    }
    out << endl;
    generate_deserialize_field(out, field, "");
    indent(out) << field->get_name() << endl;
    scope_down(out);
  }

  indent(out) << "else ->" << endl;
  indent(out) << "  throw java.lang.IllegalStateException(\"setField wasn't null, but didn't match any "
                 "of the case statements!\")" << endl;

  indent_down();
  indent(out) << "}" << endl;

  indent_down();
  indent(out) << "} else {" << endl;
  indent_up();
  indent(out)
          << "throw org.apache.thrift.protocol.TProtocolException(\"Couldn't find a field with field id \" + fieldID)"
          << endl;
  indent_down();
  indent(out) << "}" << endl;
  indent_down();
  indent(out) << "}" << endl;
}

void t_kotlin_generator::generate_tuple_scheme_write_value(ostream &out, t_struct *tstruct) {
  indent(out) << "override suspend fun tupleSchemeWriteValue(oprot: org.apache.thrift.protocol.TProtocol) {" << endl;

  indent_up();

  indent(out) << "when (uSetField) {" << endl;
  indent_up();

  const vector<t_field *> &members = tstruct->get_members();
  vector<t_field *>::const_iterator m_iter;

  for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
    t_field *field = (*m_iter);

    indent(out) << "Fields." << constant_name(field->get_name()) << " -> {" << endl;
    indent_up();
    indent(out) << "val " << field->get_name() << ":" + type_name(field->get_type(), true, false) << " = "
                << "uFieldValue as " << type_name(field->get_type(), true, false) << endl;
    generate_serialize_field(out, field, "");
    indent(out) << "return" << endl;
    scope_down(out);
  }

  indent(out) << "else -> " << endl;
  indent(out) << "  throw java.lang.IllegalStateException(\"Cannot write union with unknown field $uSetField\")" << endl;

  indent_down();
  indent(out) << "}" << endl;

  indent_down();

  indent(out) << "}" << endl;
}

void t_kotlin_generator::generate_get_field_desc(ostream &out, t_struct *tstruct) {
  indent(out) << "override fun getFieldDesc(setField:Fields?):org.apache.thrift.protocol.TField {"
              << endl;
  indent_up();

  const vector<t_field *> &members = tstruct->get_members();
  vector<t_field *>::const_iterator m_iter;

  indent(out) << "return when (setField) {" << endl;
  indent_up();

  for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
    t_field *field = (*m_iter);
    indent(out) << "Fields." << constant_name(field->get_name()) << " -> " << endl;
    indent(out) << "  return " << constant_name(field->get_name()) << "_FIELD_DESC" << endl;
  }

  indent(out) << "else -> " << endl;
  indent(out) << "  throw java.lang.IllegalArgumentException(\"Unknown field id $setField\")" << endl;

  indent_down();
  indent(out) << "}" << endl;

  indent_down();
  indent(out) << "}" << endl;
}

void t_kotlin_generator::generate_get_struct_desc(ostream &out, t_struct *tstruct) {
  (void) tstruct;
  indent(out) << "override fun getStructDesc():org.apache.thrift.protocol.TStruct {" << endl;
  indent(out) << "  return STRUCT_DESC" << endl;
  indent(out) << "}" << endl;
}

void t_kotlin_generator::generate_union_comparisons(ostream &out, t_struct *tstruct) {
  // equality
  indent(out) << "override fun equals(other: Any?):Boolean {" << endl;
  indent(out) << "  if (other is " << tstruct->get_name() << ") {" << endl;
  indent(out) << "    return equals(other)" << endl;
  indent(out) << "  } else {" << endl;
  indent(out) << "    return false" << endl;
  indent(out) << "  }" << endl;
  indent(out) << "}" << endl;

  out << endl;

  indent(out) << "fun equals(other:" << tstruct->get_name() << "?):Boolean {" << endl;
  indent(out) << "  return other != null && uSetField == other.uSetField && "
                 "uFieldValue?.equals(other.uFieldValue) ?: false" << endl;
  indent(out) << "}" << endl;
  out << endl;

  indent(out) << "override fun compareTo(other:" << type_name(tstruct) << "):Int {" << endl;
  indent(out) << "  val lastComparison:Int = org.apache.thrift.TBaseHelper.compareTo(uSetField, "
                 "other.uSetField)" << endl;
  indent(out) << "  if (lastComparison == 0) {" << endl;
  indent(out) << "    return org.apache.thrift.TBaseHelper.compareTo(uFieldValue, "
                 "other.uFieldValue)" << endl;
  indent(out) << "  }" << endl;
  indent(out) << "  return lastComparison" << endl;
  indent(out) << "}" << endl;
}

void t_kotlin_generator::generate_union_hashcode(ostream &out, t_struct *tstruct) {
  (void) tstruct;
  indent(out) << "override fun hashCode():Int {" << endl;
  indent(out) << "  val list:ArrayList<Any?> = ArrayList()" << endl;
  indent(out) << "  list.add(this::class.java.getName())" << endl;
  indent(out) << "  val setField: org.apache.thrift.TFieldIdEnum? = uSetField" << endl;
  indent(out) << "  if (setField != null) {" << endl;
  indent(out) << "    list.add(setField.thriftFieldId)" << endl;
  indent(out) << "    val value: Any? = uFieldValue" << endl;
  indent(out) << "    if (value is org.apache.thrift.TEnum) {" << endl;
  indent(out) << "      list.add((uFieldValue as org.apache.thrift.TEnum).value)" << endl;
  indent(out) << "    } else {" << endl;
  indent(out) << "      list.add(value)" << endl;
  indent(out) << "    }" << endl;
  indent(out) << "  }" << endl;
  indent(out) << "  return list.hashCode()" << endl;
  indent(out) << "}" << endl;
}

/**
 * kotlin struct definition. This has various parameters, as it could be
 * generated standalone or inside another class as a helper. If it
 * is a helper than it is a static class.
 *
 * @param tstruct      The struct definition
 * @param is_exception Is this an exception?
 * @param in_class     If inside a class, needs to be static class
 * @param is_result    If this is a result it needs a different writer
 */
void t_kotlin_generator::generate_kotlin_struct_definition(ostream &out,
                                                           t_struct *tstruct,
                                                           bool is_exception,
                                                           bool in_class,
                                                           bool is_result) {
  generate_java_doc(out, tstruct);
  ostringstream companion_out;

  bool is_const = (tstruct->annotations_.find("const") != tstruct->annotations_.end());
  bool is_deprecated = this->is_deprecated(tstruct->annotations_);

  if (!in_class) {
    generate_java_generated_annotation(out);
  }

  if (is_deprecated) {
    indent(out) << "@Deprecated" << endl;
  }
  indent(out) << "class " << tstruct->get_name() << "( " << endl;

  // Members are public
  indent_up();
  generate_private_variables(out, tstruct);
  indent_down();
  out << ")";
  if (is_exception) {
    out << ": org.apache.thrift.TException(), ";
  } else {
    out << ":";
  }
  out << " org.apache.thrift.TBase<" << tstruct->get_name() << ", " << tstruct->get_name()
      << ".Fields>, java.io.Serializable, Cloneable, Comparable<" << tstruct->get_name() << ">";

  out << " ";

  scope_up(out);

  indent(companion_out) << "companion object {" << endl;
  indent_up();
  generate_struct_desc(companion_out, tstruct);

  companion_out << endl;

  generate_field_descs(companion_out, tstruct);

  companion_out << endl;

  generate_scheme_map(companion_out, tstruct);
  indent_down();

  out << endl;

  generate_field_name_constants(out, tstruct);

  // isset data
  const vector<t_field *> &members = tstruct->get_members();
  vector<t_field *>::const_iterator m_iter;
  if (!members.empty()) {
    out << endl;

    indent_up();
    indent(companion_out) << "// isset id assignments" << endl;

    int i = 0;
    int optionals = 0;
    for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
      if ((*m_iter)->get_req() == t_field::T_OPTIONAL) {
        optionals++;
      }
      if (!type_can_be_null((*m_iter)->get_type())) {
        indent(companion_out) << "private val " << isset_field_id(*m_iter) << ": Int = " << i << endl;
        i++;
      }
    }
    indent_down();

    std::string primitiveType;
    switch (needs_isset(tstruct, &primitiveType)) {
      case ISSET_NONE:
        break;
      case ISSET_PRIMITIVE:
        indent(out) << "private var " << " __isset_bitfield: " << primitiveType << " = 0" << endl;
        break;
      case ISSET_BITSET:
        indent(out) << "private var __isset_bit_vector: java.util.BitSet = java.util.BitSet(" << i << ")" << endl;
        break;
    }

    if (optionals > 0) {
      std::string output_string = "private val optionals: Array<Fields> = arrayOf(";
      for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
        if ((*m_iter)->get_req() == t_field::T_OPTIONAL) {
          output_string += "Fields." + constant_name((*m_iter)->get_name()) + ",";
        }
      }
      indent(out) << output_string.substr(0, output_string.length() - 1) << ")" << endl;
    }
  }

  generate_kotlin_meta_data_map(out, tstruct);

  generate_constructor(out, tstruct);

  // copy constructor
  indent(out) << "/**" << endl;
  indent(out) << " * Performs a deep copy on <i>other</i>." << endl;
  indent(out) << " */" << endl;
  indent(out) << "constructor(other: " << tstruct->get_name() << ") : this() {"
              << endl;
  indent_up();

  switch (needs_isset(tstruct)) {
    case ISSET_NONE:
      break;
    case ISSET_PRIMITIVE:
      indent(out) << "__isset_bitfield = other.__isset_bitfield;" << endl;
      break;
    case ISSET_BITSET:
      indent(out) << "__isset_bit_vector.clear()" << endl;
      indent(out) << "__isset_bit_vector.or(other.__isset_bit_vector)" << endl;
      break;
  }

  for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
    t_field *field = (*m_iter);
    std::string field_name = field->get_name();
    t_type *type = field->get_type()->get_true_type();
    bool can_be_null = type_can_be_null(type);

    if (can_be_null) {
      indent(out) << "if (other." << generate_isset_check(field) << ") {" << endl;
      indent_up();
    }

    if (type->is_container()) {
      generate_deep_copy_container(out, "other", field_name, "__this__" + field_name, type);
      indent(out) << "this." << field_name << " = __this__" << field_name << endl;
    } else {
      indent(out) << "this." << field_name << " = ";
      generate_deep_copy_non_container(out, "other." + field_name, field_name, type);
      out << endl;
    }

    if (can_be_null) {
      indent_down();
      indent(out) << "}" << endl;
    }
  }

  indent_down();
  indent(out) << "}" << endl << endl;

  // clone method, so that you can deep copy an object when you don't know its class.
  indent(out) << "override fun deepCopy(): " << tstruct->get_name() << "{" << endl;
  indent(out) << "  return " << tstruct->get_name() << "(this)" << endl;
  indent(out) << "}" << endl << endl;

  generate_kotlin_struct_clear(out, tstruct);

  generate_kotlin_bean_boilerplate(out, tstruct);
  generate_generic_field_getters_setters(out, tstruct);
  generate_generic_isset_method(out, tstruct);

  generate_kotlin_struct_equality(out, tstruct);
  generate_kotlin_struct_compare_to(out, tstruct);
  generate_kotlin_struct_field_by_id(out, tstruct);

  generate_kotlin_struct_reader(out, tstruct);
  if (is_result) {
    generate_kotlin_struct_result_writer(out, tstruct);
  } else {
    generate_kotlin_struct_writer(out, tstruct);
  }
  generate_kotlin_struct_tostring(out, tstruct);
  generate_kotlin_validator(out, tstruct);

  generate_kotlin_struct_write_object(out, tstruct);
  generate_kotlin_struct_read_object(out, tstruct);

  generate_kotlin_struct_standard_scheme(out, tstruct, is_result);
  generate_kotlin_struct_tuple_scheme(out, tstruct);
  generate_kotlin_scheme_lookup(companion_out);

  companion_out << indent() << "}" << endl;
  out << endl << companion_out.str();
  scope_down(out);
  out << endl;
}

/**
 * generates parcelable interface implementation
 */
void t_kotlin_generator::generate_kotlin_struct_parcelable(ostream &out, t_struct *tstruct) {
  string tname = tstruct->get_name();

  const vector<t_field *> &members = tstruct->get_members();
  vector<t_field *>::const_iterator m_iter;

  out << indent() << "@Override" << endl << indent()
      << "public void writeToParcel(android.os.Parcel out, int flags) {" << endl;
  indent_up();
  string bitsetPrimitiveType = "";
  switch (needs_isset(tstruct, &bitsetPrimitiveType)) {
    case ISSET_NONE:
      break;
    case ISSET_PRIMITIVE:
      indent(out) << "//primitive bitfield of type: " << bitsetPrimitiveType << endl;
      if (bitsetPrimitiveType == "byte") {
        indent(out) << "out.writeByte(__isset_bitfield);" << endl;
      } else if (bitsetPrimitiveType == "short") {
        indent(out) << "out.writeInt(Short(__isset_bitfield).intValue());" << endl;
      } else if (bitsetPrimitiveType == "int") {
        indent(out) << "out.writeInt(__isset_bitfield);" << endl;
      } else if (bitsetPrimitiveType == "long") {
        indent(out) << "out.writeLong(__isset_bitfield);" << endl;
      }
      out << endl;
      break;
    case ISSET_BITSET:
      indent(out) << "//BitSet" << endl;
      indent(out) << "out.writeSerializable(__isset_bit_vector);" << endl;
      out << endl;
      break;
  }
  for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
    t_type *t = get_true_type((*m_iter)->get_type());
    string name = (*m_iter)->get_name();

    if (t->is_struct()) {
      indent(out) << "out.writeParcelable(" << name << ", flags);" << endl;
    } else if (type_name(t) == "float") {
      indent(out) << "out.writeFloat(" << name << ");" << endl;
    } else if (t->is_enum()) {
      indent(out) << "out.writeInt(" << name << " != null ? " << name << ".getValue() : -1);" << endl;
    } else if (t->is_list()) {
      if (((t_list *) t)->get_elem_type()->get_true_type()->is_struct()) {
        indent(out) << "out.writeTypedList(" << name << ");" << endl;
      } else {
        indent(out) << "out.writeList(" << name << ");" << endl;
      }
    } else if (t->is_map()) {
      indent(out) << "out.writeMap(" << name << ");" << endl;
    } else if (t->is_base_type()) {
      if (t->is_binary()) {
        indent(out) << "out.writeInt(" << name << "!=null ? 1 : 0);" << endl;
        indent(out) << "if(" << name << " != null) { " << endl;
        indent_up();
        indent(out) << "out.writeByteArray(" << name << ".array(), " << name << ".position() + "
                    << name << ".arrayOffset(), " << name << ".limit() - " << name
                    << ".position() );" << endl;
        scope_down(out);
      } else {
        switch (((t_base_type *) t)->get_base()) {
          case t_base_type::TYPE_I16:
            indent(out) << "out.writeInt(Short(" << name << ").intValue());" << endl;
            break;
          case t_base_type::TYPE_I32:
            indent(out) << "out.writeInt(" << name << ");" << endl;
            break;
          case t_base_type::TYPE_I64:
            indent(out) << "out.writeLong(" << name << ");" << endl;
            break;
          case t_base_type::TYPE_BOOL:
            indent(out) << "out.writeInt(" << name << " ? 1 : 0);" << endl;
            break;
          case t_base_type::TYPE_I8:
            indent(out) << "out.writeByte(" << name << ");" << endl;
            break;
          case t_base_type::TYPE_DOUBLE:
            indent(out) << "out.writeDouble(" << name << ");" << endl;
            break;
          case t_base_type::TYPE_STRING:
            indent(out) << "out.writeString(" << name << ");" << endl;
            break;
          case t_base_type::TYPE_VOID:
            break;
        }
      }
    }
  }
  scope_down(out);
  out << endl;

  out << indent() << "@Override" << endl << indent() << "public int describeContents() {" << endl;
  indent_up();
  out << indent() << "return 0;" << endl;
  scope_down(out);
  out << endl;

  indent(out) << "public " << tname << "(android.os.Parcel in) {" << endl;
  indent_up();
  // read in the required bitfield
  switch (needs_isset(tstruct, &bitsetPrimitiveType)) {
    case ISSET_NONE:
      break;
    case ISSET_PRIMITIVE:
      indent(out) << "//primitive bitfield of type: " << bitsetPrimitiveType << endl;
      if (bitsetPrimitiveType == "byte") {
        indent(out) << "__isset_bitfield = in.readByte();" << endl;
      } else if (bitsetPrimitiveType == "short") {
        indent(out) << "__isset_bitfield = (short) in.readInt();" << endl;
      } else if (bitsetPrimitiveType == "int") {
        indent(out) << "__isset_bitfield = in.readInt();" << endl;
      } else if (bitsetPrimitiveType == "long") {
        indent(out) << "__isset_bitfield = in.readLong();" << endl;
      }
      out << endl;
      break;
    case ISSET_BITSET:
      indent(out) << "//BitSet" << endl;
      indent(out) << "__isset_bit_vector = (kotlin.util.BitSet) in.readSerializable();" << endl;
      out << endl;
      break;
  }
  // read all the fields
  for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
    t_type *t = get_true_type((*m_iter)->get_type());
    string name = (*m_iter)->get_name();
    string prefix = "this." + name;

    if (t->is_struct()) {
      indent(out) << prefix << "= in.readParcelable(" << tname << ".class.getClassLoader());"
                  << endl;
    } else if (t->is_enum()) {
      indent(out) << prefix << " = " << type_name(t) << ".findByValue(in.readInt());" << endl;
    } else if (t->is_list()) {
      t_list *list = (t_list *) t;
      indent(out) << prefix << " = " << type_name(t, false, true) << "();" << endl;
      if (list->get_elem_type()->get_true_type()->is_struct()) {
        indent(out) << "in.readTypedList(" << prefix << ", " << type_name(list->get_elem_type())
                    << ".CREATOR);" << endl;
      } else {
        indent(out) << "in.readList(" << prefix << ", " << tname << ".class.getClassLoader());"
                    << endl;
      }
    } else if (t->is_map()) {
      indent(out) << prefix << " = " << type_name(t, false, true) << "();" << endl;
      indent(out) << " in.readMap(" << prefix << ", " << tname << ".class.getClassLoader());"
                  << endl;
    } else if (type_name(t) == "float") {
      indent(out) << prefix << " = in.readFloat();" << endl;
    } else if (t->is_base_type()) {
      t_base_type *bt = (t_base_type *) t;
      if (bt->is_binary()) {
        indent(out) << "if(in.readInt()==1) {" << endl;
        indent_up();
        indent(out) << prefix << " = java.nio.ByteBuffer.wrap(in.createByteArray());" << endl;
        scope_down(out);
      } else {
        switch (bt->get_base()) {
          case t_base_type::TYPE_I16:
            indent(out) << prefix << " = (short) in.readInt();" << endl;
            break;
          case t_base_type::TYPE_I32:
            indent(out) << prefix << " = in.readInt();" << endl;
            break;
          case t_base_type::TYPE_I64:
            indent(out) << prefix << " = in.readLong();" << endl;
            break;
          case t_base_type::TYPE_BOOL:
            indent(out) << prefix << " = (in.readInt()==1);" << endl;
            break;
          case t_base_type::TYPE_I8:
            indent(out) << prefix << " = in.readByte();" << endl;
            break;
          case t_base_type::TYPE_DOUBLE:
            indent(out) << prefix << " = in.readDouble();" << endl;
            break;
          case t_base_type::TYPE_STRING:
            indent(out) << prefix << "= in.readString();" << endl;
            break;
          case t_base_type::TYPE_VOID:
            break;
        }
      }
    }
  }

  scope_down(out);
  out << endl;

  indent(out) << "public static final android.os.Parcelable.Creator<" << tname
              << "> CREATOR = android.os.Parcelable.Creator<" << tname << ">() {" << endl;
  indent_up();

  indent(out) << "@Override" << endl << indent() << "public " << tname << "[] newArray(int size) {"
              << endl;
  indent_up();
  indent(out) << "return " << tname << "[size];" << endl;
  scope_down(out);
  out << endl;

  indent(out) << "@Override" << endl << indent() << "public " << tname
              << " createFromParcel(android.os.Parcel in) {" << endl;
  indent_up();
  indent(out) << "return " << tname << "(in);" << endl;
  scope_down(out);

  indent_down();
  indent(out) << "};" << endl;
  out << endl;
}

/**
 * Generates equals methods and a hashCode method for a structure.
 *
 * @param tstruct The struct definition
 */
void t_kotlin_generator::generate_kotlin_struct_equality(ostream &out, t_struct *tstruct) {
  out << indent() << "override fun equals(other: Any?): Boolean {"
      << endl;
  indent_up();
  out << indent() << "if (other is " << tstruct->get_name() << ")" << endl
      << indent() << indent() << "return this.equals(other)" << endl
      << indent() << "return false" << endl;
  scope_down(out);
  out << endl;

  out << indent() << "fun equals(other: " << tstruct->get_name() << kotlin_nullable_annotation() << "):Boolean {"
      << endl;
  indent_up();
  out << indent() << "if (other == null)" << endl << indent() << "  return false" << endl
      << indent() << "if (this == other)" << endl << indent() << "  return true" << endl;

  const vector<t_field *> &members = tstruct->get_members();
  vector<t_field *>::const_iterator m_iter;
  for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
    out << endl;

    t_type *t = get_true_type((*m_iter)->get_type());
    // Most existing Thrift code does not use isset or optional/required,
    // so we treat "default" fields as required.
    bool is_optional = (*m_iter)->get_req() == t_field::T_OPTIONAL;
    bool can_be_null = type_can_be_null(t);
    string name = (*m_iter)->get_name();

    string this_present = "true";
    string that_present = "true";
    string unequal;

    if (is_optional || can_be_null) {
      this_present += " && this." + generate_isset_check(*m_iter);
      that_present += " && other." + generate_isset_check(*m_iter);
    }

    out << indent() << "val this_present_" << name << ": Boolean = " << this_present << endl
        << indent() << "val other_present_" << name << ": Boolean = " << that_present << endl
        << indent() << "if ("
        << "this_present_" << name << " || other_present_" << name << ") {" << endl;
    indent_up();
    out << indent() << "if (!("
        << "this_present_" << name << " && other_present_" << name << "))" << endl << indent()
        << "  return false;" << endl;

    if (t->is_binary()) {
      unequal = "!this." + name + "!!.equals(other." + name + ")";
    } else if (can_be_null) {
      unequal = "!this." + name + "!!.equals(other." + name + ")";
    } else {
      unequal = "this." + name + " != other." + name;
    }

    out << indent() << "if (" << unequal << ")" << endl << indent() << "  return false;" << endl;

    scope_down(out);
  }
  out << endl;
  indent(out) << "return true;" << endl;
  scope_down(out);
  out << endl;

  const int MUL = 8191; // HashCode multiplier
  const int B_YES = 131071;
  const int B_NO = 524287;
  out << indent() << "override fun hashCode():Int {" << endl;
  indent_up();
  indent(out) << "var hashCode: Int = 1" << endl;

  for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
    out << endl;

    t_type *t = get_true_type((*m_iter)->get_type());
    bool is_optional = (*m_iter)->get_req() == t_field::T_OPTIONAL;
    bool can_be_null = type_can_be_null(t);
    string name = (*m_iter)->get_name();

    if (is_optional || can_be_null) {
      indent(out) << "hashCode = hashCode * " << MUL << " + (if(" << generate_isset_check(*m_iter)
                  << ") " << B_YES << " else " << B_NO << ")" << endl;
    }

    if (is_optional || can_be_null) {
      indent(out) << "if (" + generate_isset_check(*m_iter) + ")" << endl;
      indent_up();
    }

    if (t->is_enum()) {
      indent(out) << "hashCode = hashCode * " << MUL << " + " << name << "!!.value" << endl;
    } else if (t->is_base_type()) {
      switch (((t_base_type *) t)->get_base()) {
        case t_base_type::TYPE_STRING:
          indent(out) << "hashCode = hashCode * " << MUL << " + " << name << ".hashCode()" << endl;
          break;
        case t_base_type::TYPE_BOOL:
          indent(out) << "hashCode = hashCode * " << MUL << " + (if(" << name << ") "
                      << B_YES << " else " << B_NO << ")" << endl;
          break;
        case t_base_type::TYPE_I8:
          indent(out) << "hashCode = hashCode * " << MUL << " + " << name << ".toInt()" << endl;
          break;
        case t_base_type::TYPE_I16:
        case t_base_type::TYPE_I32:
          indent(out) << "hashCode = hashCode * " << MUL << " + " << name << endl;
          break;
        case t_base_type::TYPE_I64:
        case t_base_type::TYPE_DOUBLE:
          indent(out) << "hashCode = hashCode * " << MUL << " + org.apache.thrift.TBaseHelper.hashCode(" << name << ")"
                      << endl;
          break;
        case t_base_type::TYPE_VOID:
          throw std::logic_error("compiler error: a struct field cannot be void");
        default:
          throw std::logic_error("compiler error: the following base type has no hashcode generator: " +
                                 t_base_type::t_base_name(((t_base_type *) t)->get_base()));
      }
    } else {
      indent(out) << "hashCode = hashCode * " << MUL << " + " << name << ".hashCode()" << endl;
    }

    if (is_optional || can_be_null) {
      indent_down();
    }
  }

  out << endl;
  indent(out) << "return hashCode" << endl;
  indent_down();
  indent(out) << "}" << endl << endl;
}

void t_kotlin_generator::generate_kotlin_struct_compare_to(ostream &out, t_struct *tstruct) {
  indent(out) << "override fun compareTo(other: " << type_name(tstruct) << "):Int {" << endl;
  indent_up();

  indent(out) << "if (!javaClass.equals(other.javaClass)) {" << endl;
  indent(out) << "  return javaClass.getName().compareTo(other.javaClass.getName())" << endl;
  indent(out) << "}" << endl;
  out << endl;

  indent(out) << "var lastComparison:Int = 0" << endl;
  out << endl;

  const vector<t_field *> &members = tstruct->get_members();
  vector<t_field *>::const_iterator m_iter;
  for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
    t_field *field = *m_iter;
    string field_name = field->get_name();
    indent(out) << "lastComparison = java.lang.Boolean.compare(" << generate_isset_check(field)
                << ", other." << generate_isset_check(field) << ");" << endl;
    indent(out) << "if (lastComparison != 0) {" << endl;
    indent(out) << "  return lastComparison;" << endl;
    indent(out) << "}" << endl;

    indent(out) << "if (" << generate_isset_check(field) << ") {" << endl;
    indent(out) << "  lastComparison = org.apache.thrift.TBaseHelper.compareTo(this."
                << field_name << ", other." << field_name << ");" << endl;
    indent(out) << "  if (lastComparison != 0) {" << endl;
    indent(out) << "    return lastComparison;" << endl;
    indent(out) << "  }" << endl;
    indent(out) << "}" << endl;
  }

  indent(out) << "return 0;" << endl;

  indent_down();
  indent(out) << "}" << endl << endl;
}

/**
 * Generates a function to read all the fields of the struct.
 *
 * @param tstruct The struct definition
 */
void t_kotlin_generator::generate_kotlin_struct_reader(ostream &out, t_struct *tstruct) {
  (void) tstruct;
  indent(out) << "override suspend fun read(iprot: org.apache.thrift.protocol.TProtocol) {" << endl;
  indent_up();
  indent(out) << "scheme<" << tstruct->get_name() << ">(iprot).read(iprot, this)" << endl;
  indent_down();
  indent(out) << "}" << endl << endl;
}

// generates kotlin method to perform various checks
// (e.g. check that all required fields are set)
void t_kotlin_generator::generate_kotlin_validator(ostream &out, t_struct *tstruct) {
  indent(out) << "fun validate() {" << endl;
  indent_up();

  const vector<t_field *> &fields = tstruct->get_members();
  vector<t_field *>::const_iterator f_iter;

  out << indent() << "// check for required fields" << endl;
  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    if ((*f_iter)->get_req() == t_field::T_REQUIRED) {
      if (type_can_be_null((*f_iter)->get_type())) {
        indent(out) << "if (" << (*f_iter)->get_name() << " == null) {" << endl;
        indent(out)
                << "  throw org.apache.thrift.protocol.TProtocolException(\"Required field '"
                << (*f_iter)->get_name() << "' was not present! Struct: \" + toString());" << endl;
        indent(out) << "}" << endl;
      }
    }
  }

  out << indent() << "// check for sub-struct validity" << endl;
  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    t_type *type = (*f_iter)->get_type();
    if (type->is_struct() && !((t_struct *) type)->is_union()) {
      out << indent() << (*f_iter)->get_name() << kotlin_nullable_annotation() << ".validate();" << endl;
    }
  }

  indent_down();
  indent(out) << "}" << endl << endl;
}

/**
 * Generates a function to write all the fields of the struct
 *
 * @param tstruct The struct definition
 */
void t_kotlin_generator::generate_kotlin_struct_writer(ostream &out, t_struct *tstruct) {
  (void) tstruct;
  indent(out) << "override suspend fun write(oprot: org.apache.thrift.protocol.TProtocol) {" << endl;
  indent_up();
  indent(out) << "scheme<" << tstruct->get_name() << ">(oprot).write(oprot, this)" << endl;

  indent_down();
  indent(out) << "}" << endl << endl;
}

/**
 * Generates a function to write all the fields of the struct,
 * which is a function result. These fields are only written
 * if they are set in the Isset array, and only one of them
 * can be set at a time.
 *
 * @param tstruct The struct definition
 */
void t_kotlin_generator::generate_kotlin_struct_result_writer(ostream &out, t_struct *tstruct) {
  (void) tstruct;
  indent(out) << "override suspend fun write(oprot: org.apache.thrift.protocol.TProtocol) {" << endl;
  indent_up();
  indent(out) << "scheme<" << tstruct->get_name() << ">(oprot).write(oprot, this)" << endl;

  indent_down();
  indent(out) << "}" << endl << endl;
}

void t_kotlin_generator::generate_kotlin_struct_field_by_id(ostream &out, t_struct *tstruct) {
  (void) tstruct;
  indent(out) << "override fun fieldForId(fieldId: Short):Fields?" << " {" << endl;
  indent(out) << "  return Fields.findByThriftId(fieldId)" << endl;
  indent(out) << "}" << endl << endl;
}

void t_kotlin_generator::generate_reflection_getters(ostringstream &out,
                                                     t_type *type,
                                                     const string &field_name,
                                                     const string &cap_name) {
  indent_up();
  indent(out) << "Fields." << constant_name(field_name) << " -> "
              << (type->is_bool() ? "is" : "get") << cap_name << "()" << endl;
  indent_down();
}

void t_kotlin_generator::generate_reflection_setters(ostringstream &out,
                                                     t_type *type,
                                                     const string &field_name,
                                                     const string &cap_name) {
  const bool is_binary = type->is_binary();
  indent(out) << "Fields." << constant_name(field_name) << " ->" << endl;
  indent_up();
  indent(out) << "if (value == null) {" << endl;
  indent(out) << "  unset" << get_cap_name(field_name) << "()" << endl;
  indent(out) << "} else {" << endl;
  if (is_binary) {
    indent_up();
    indent(out) << "if (value is ByteArray) {" << endl;
    indent(out) << "  set" << cap_name << "(value)" << endl;
    indent(out) << "} else {" << endl;
  }
  indent(out) << "  set" << cap_name << "(value as " << type_name(type, true, false) << ")" << endl;
  if (is_binary) {
    indent(out) << "}" << endl;
    indent_down();
  }
  indent(out) << "}" << endl << endl;

  indent_down();
}

void t_kotlin_generator::generate_generic_field_getters_setters(std::ostream &out,
                                                                t_struct *tstruct) {
  std::ostringstream getter_stream;
  std::ostringstream setter_stream;

  // build up the bodies of both the getter and setter at once
  const vector<t_field *> &fields = tstruct->get_members();
  vector<t_field *>::const_iterator f_iter;
  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    t_field *field = *f_iter;
    t_type *type = get_true_type(field->get_type());
    std::string field_name = field->get_name();
    std::string cap_name = get_cap_name(field_name);

    indent_up();
    generate_reflection_setters(setter_stream, type, field_name, cap_name);
    generate_reflection_getters(getter_stream, type, field_name, cap_name);
    indent_down();
  }

  // create the setter
  const string setter_string = setter_stream.str();
  indent(out) << "override fun setFieldValue(field: Fields, "
                << " value: Any" << kotlin_nullable_annotation() << ") {" << endl;
  indent_up();
  if(setter_string.length() > 0) {
    indent(out) << "  when (field) {" << endl;
    out << setter_string;
    indent(out) << "  }" << endl;
  }
  indent_down();
  indent(out) << "}" << endl << endl;

  // create the getter
  const string getter_string = getter_stream.str();
  indent(out) << "override fun getFieldValue(field: Fields): Any" << kotlin_nullable_annotation() << " {" << endl;
  indent_up();
  if(getter_string.length() > 0) {
    indent(out) << "return when(field) {" << endl;
    out << getter_string;
    indent(out) << "}" << endl;
  } else {
    indent(out) << "return false" << endl;
  }
  indent_down();
  indent(out) << "}" << endl << endl;
}

// Creates a generic isSet method that takes the field number as argument
void t_kotlin_generator::generate_generic_isset_method(std::ostream &out, t_struct *tstruct) {
  const vector<t_field *> &fields = tstruct->get_members();
  vector<t_field *>::const_iterator f_iter;

  // create the isSet method
  indent(out) << "// Returns true if field corresponding to fieldID is set (has been assigned a "
                 "value) and false otherwise" << endl;
  indent(out) << "override fun isSet(field: Fields): Boolean {" << endl;
  indent_up();

  if(!fields.empty()) {
    indent(out) << "return when (field) {" << endl;
    indent_up();
    for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
      t_field *field = *f_iter;
      indent(out) << "Fields." << constant_name(field->get_name()) << "->"
                  << generate_isset_check(field) << endl;
    }
    indent_down();
    indent(out) << "}" << endl;
  } else {
    indent(out) << "return false" << endl;
  }
  indent_down();
  indent(out) << "}" << endl << endl;
}

/**
 * Generates a set of kotlin Bean boilerplate functions (setters, getters, etc.)
 * for the given struct.
 *
 * @param tstruct The struct definition
 */
void t_kotlin_generator::generate_kotlin_bean_boilerplate(ostream &out, t_struct *tstruct) {
  isset_type issetType = needs_isset(tstruct);
  const vector<t_field *> &fields = tstruct->get_members();
  vector<t_field *>::const_iterator f_iter;
  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    t_field *field = *f_iter;
    t_type *type = get_true_type(field->get_type());
    std::string field_name = field->get_name();
    std::string cap_name = get_cap_name(field_name);
    bool optional = field->get_req() == t_field::T_OPTIONAL;
    bool is_deprecated = this->is_deprecated(field->annotations_);

    if (type->is_container()) {
      // Method to return the size of the collection
      if (optional) {
        if (is_deprecated) {
          indent(out) << "@Deprecated" << endl;
        }
        indent(out) << "fun get" << cap_name;
        out << get_cap_name("size(): org.apache.thrift.Option<Int>  {") << endl;

        indent_up();
        indent(out) << "if (this." << field_name << " == null) {" << endl;
        indent_up();
        indent(out) << "return org.apache.thrift.Option.none();" << endl;
        indent_down();
        indent(out) << "} else {" << endl;
        indent_up();
        indent(out) << "return org.apache.thrift.Option.some(this." << field_name << "!!.size)" << endl;
        indent_down();
        indent(out) << "}" << endl;
        indent_down();
        indent(out) << "}" << endl << endl;
      } else {
        if (is_deprecated) {
          indent(out) << "@Deprecated" << endl;
        }
        indent(out) << "fun get" << cap_name;
        out << get_cap_name("size():Int {") << endl;

        indent_up();
        indent(out) << "return this." << field_name << "?.size ?: 0" << endl;
        indent_down();
        indent(out) << "}" << endl << endl;
      }
    }

    if (type->is_set() || type->is_list()) {
      t_type *element_type;
      if (type->is_set()) {
        element_type = ((t_set *) type)->get_elem_type();
      } else {
        element_type = ((t_list *) type)->get_elem_type();
      }

      // Iterator getter for sets and lists
      if (optional) {
        if (is_deprecated) {
          indent(out) << "@Deprecated" << endl;
        }
        indent(out) << "fun get" << cap_name << get_cap_name("iterator():")
                    << "org.apache.thrift.Option<Iterator<" << type_name(element_type, true, false) << ">> {" << endl;

        indent_up();
        indent(out) << "if (this." << field_name << " == null) {" << endl;
        indent_up();
        indent(out) << "return org.apache.thrift.Option.none()" << endl;
        indent_down();
        indent(out) << "} else {" << endl;
        indent_up();
        indent(out) << "return org.apache.thrift.Option.some(this." << field_name << "!!.iterator())" << endl;
        indent_down();
        indent(out) << "}" << endl;
        indent_down();
        indent(out) << "}" << endl << endl;
      } else {
        if (is_deprecated) {
          indent(out) << "@Deprecated" << endl;
        }
        indent(out) << "fun get" << cap_name << "Iterator():Iterator<"
                    << type_name(element_type, true, false) << ">? {" << endl;

        indent_up();
        indent(out) << "return this." << field_name << "?.iterator()" << endl;
        indent_down();
        indent(out) << "}" << endl << endl;
      }

      // Add to set or list, create if the set/list is null
      if (is_deprecated) {
        indent(out) << "@Deprecated" << endl;
      }
      indent(out) << "fun addTo" << cap_name << "(elem:" << type_name(element_type) << ") {" << endl;

      indent_up();
      indent(out) << "if (this." << field_name << " == null) {" << endl;
      indent_up();
      indent(out) << "this." << field_name;
      if (is_enum_set(type)) {
        out << " = " << type_name(type, false, true, true) << ".noneOf(" << inner_enum_type_name(type) << ")" << endl;
      } else {
        out << " = " << type_name(type, false, true) << "()" << endl;
      }
      indent_down();
      indent(out) << "}" << endl;
      indent(out) << "this." << field_name << "?.add(elem)" << endl;
      indent_down();
      indent(out) << "}" << endl << endl;
    } else if (type->is_map()) {
      // Put to map
      t_type *key_type = ((t_map *) type)->get_key_type();
      t_type *val_type = ((t_map *) type)->get_val_type();

      if (is_deprecated) {
        indent(out) << "@Deprecated" << endl;
      }
      indent(out) << "fun putTo" << cap_name << "(key: " << type_name(key_type)
                  << ", value: " << type_name(val_type) << ") {" << endl;

      indent_up();
      indent(out) << "if (this." << field_name << " == null) {" << endl;
      indent_up();
      std::string constructor_args;
      if (is_enum_map(type)) {
        constructor_args = inner_enum_type_name(type);
      }
      indent(out) << "this." << field_name << " = " << type_name(type, false, true) << "(" << constructor_args
                  << ")"
                  << endl;
      indent_down();
      indent(out) << "}" << endl;
      indent(out) << "this." << field_name << "?.put(key, value)" << endl;
      indent_down();
      indent(out) << "}" << endl << endl;
    }

    // Simple getter
    generate_java_doc(out, field);
    if (type->is_binary()) {
      if (is_deprecated) {
        indent(out) << "@Deprecated" << endl;
      }
      indent(out) << "fun get" << cap_name << "():ByteArray? {" << endl;
      indent(out) << "  set" << cap_name << "(org.apache.thrift.TBaseHelper.rightSize("
                  << field_name << "))" << endl;
      indent(out) << "  return " << field_name << "?.array()"
                  << endl;
      indent(out) << "}" << endl << endl;

      indent(out) << "fun buffer" << get_cap_name("for") << cap_name << "():java.nio.ByteBuffer? {"
                  << endl;
      indent(out) << "  return org.apache.thrift.TBaseHelper.copyBinary(" << field_name << ")"
                  << endl;
      indent(out) << "}" << endl << endl;
    } else {
      if (optional) {
        if (is_deprecated) {
          indent(out) << "@Deprecated" << endl;
        }
        indent(out) << "fun ";
        if (type->is_base_type() && ((t_base_type *) type)->get_base() == t_base_type::TYPE_BOOL) {
          out << " is";
        } else {
          out << " get";
        }
        out << cap_name << "():org.apache.thrift.Option<" << type_name(type, true) << "> {" << endl;
        indent_up();

        indent(out) << "if (this.isSet" << cap_name << "()) {" << endl;
        indent_up();
        indent(out) << "return org.apache.thrift.Option.some(this." << field_name << "!!)" << endl;
        indent_down();
        indent(out) << "} else {" << endl;
        indent_up();
        indent(out) << "return org.apache.thrift.Option.none()" << endl;
        indent_down();
        indent(out) << "}" << endl;
        indent_down();
        indent(out) << "}" << endl << endl;
      } else {
        if (is_deprecated) {
          indent(out) << "@Deprecated" << endl;
        }
        indent(out) << "fun";
        if (type->is_base_type() && ((t_base_type *) type)->get_base() == t_base_type::TYPE_BOOL) {
          out << " is";
        } else {
          out << " get";
        }
        out << cap_name << "():" << type_name(type);
        if (type_can_be_null(type)) {
          out << kotlin_nullable_annotation();
        }
        out << " {" << endl;
        indent_up();
        indent(out) << "return this." << field_name << endl;
        indent_down();
        indent(out) << "}" << endl << endl;
      }
    }

    // Simple setter
    generate_java_doc(out, field);
    if (type->is_binary()) {
      if (is_deprecated) {
        indent(out) << "@Deprecated" << endl;
      }
      indent(out) << "fun ";
      out << "set" << cap_name << "(" << field_name << ": ByteArray?): " << type_name(tstruct) << " {" << endl;
      indent(out) << "  this." << field_name << " = if(" << field_name << " == null) null";

      indent(out) << " else java.nio.ByteBuffer.wrap(" << field_name << ".clone())" << endl;

      indent(out) << "  return this" << endl;
      indent(out) << "}" << endl << endl;
    }
    if (is_deprecated) {
      indent(out) << "@Deprecated" << endl;
    }
    indent(out) << "fun";
    out << " set" << cap_name << "(" << field_name << ": "
        << type_name(type) << (type_can_be_null(type) ? (kotlin_nullable_annotation()) : "")
        << "): " << type_name(tstruct) << " {" << endl;
    indent_up();
    indent(out) << "this." << field_name << " = ";
    if (type->is_binary()) {
      out << "org.apache.thrift.TBaseHelper.copyBinary(" << field_name << ")";
    } else {
      out << field_name;
    }
    out << endl;
    generate_isset_set(out, field, "");
    indent(out) << "return this" << endl;

    indent_down();
    indent(out) << "}" << endl << endl;

    // Unsetter
    if (is_deprecated) {
      indent(out) << "@Deprecated" << endl;
    }
    indent(out) << "fun unset" << cap_name << "() {" << endl;
    indent_up();
    if (type_can_be_null(type)) {
      indent(out) << "this." << field_name << " = null" << endl;
    } else if (issetType == ISSET_PRIMITIVE) {
      indent(out) << "__isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, "
                  << isset_field_id(field) << ");" << endl;
    } else {
      indent(out) << "__isset_bit_vector.clear(" << isset_field_id(field) << ")" << endl;
    }
    indent_down();
    indent(out) << "}" << endl << endl;

    // isSet method
    indent(out) << "/** Returns true if field " << field_name
                << " is set (has been assigned a value) and false otherwise */" << endl;
    if (is_deprecated) {
      indent(out) << "@Deprecated" << endl;
    }
    indent(out) << "fun is" << get_cap_name("set") << cap_name << "(): Boolean {" << endl;
    indent_up();
    if (type_can_be_null(type)) {
      indent(out) << "return this." << field_name << " != null" << endl;
    } else if (issetType == ISSET_PRIMITIVE) {
      indent(out) << "return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, "
                  << isset_field_id(field)
                  << ")" << endl;
    } else {
      indent(out) << "return __isset_bit_vector.get(" << isset_field_id(field) << ");" << endl;
    }
    indent_down();
    indent(out) << "}" << endl << endl;

    if (is_deprecated) {
      indent(out) << "@Deprecated" << endl;
    }
    indent(out) << "fun set" << cap_name << get_cap_name("isSet") << "(value: Boolean) {"
                << endl;
    indent_up();
    if (type_can_be_null(type)) {
      indent(out) << "if (!value) {" << endl;
      indent(out) << "  this." << field_name << " = null" << endl;
      indent(out) << "}" << endl;
    } else if (issetType == ISSET_PRIMITIVE) {
      indent(out) << "__isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, "
                  << isset_field_id(field) << ", value);" << endl;
    } else {
      indent(out) << "__isset_bit_vector.set(" << isset_field_id(field) << ", value)" << endl;
    }
    indent_down();
    indent(out) << "}" << endl << endl;
  }
}

/**
 * Generates a toString() method for the given struct
 *
 * @param tstruct The struct definition
 */
void t_kotlin_generator::generate_kotlin_struct_tostring(ostream &out, t_struct *tstruct) {
  out << indent() << "override fun toString():String {" << endl;
  indent_up();

  out << indent() << "val sb:java.lang.StringBuilder = java.lang.StringBuilder(\"" << tstruct->get_name() << "(\")"
      << endl;
  out << indent() << "var first:Boolean = true" << endl << endl;

  const vector<t_field *> &fields = tstruct->get_members();
  vector<t_field *>::const_iterator f_iter;
  bool first = true;
  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    bool could_be_unset = (*f_iter)->get_req() == t_field::T_OPTIONAL;
    if (could_be_unset) {
      indent(out) << "if (" << generate_isset_check(*f_iter) << ") {" << endl;
      indent_up();
    }
    t_field *field = (*f_iter);
    string field_name = field->get_name();

    if (!first) {
      indent(out) << "if (!first) sb.append(\", \")" << endl;
    }
    indent(out) << "sb.append(\"" << field_name << ":\")" << endl;
    bool can_be_null = type_can_be_null(field->get_type());
    if (can_be_null) {
      indent(out) << "if (this." << field_name << " == null) {" << endl;
      indent(out) << "  sb.append(\"null\")" << endl;
      indent(out) << "} else {" << endl;
      indent_up();
    }

    if (get_true_type(field->get_type())->is_binary())
      indent(out) << "org.apache.thrift.TBaseHelper.toString(" << field_name << "!!, sb)"
                  << endl;
    else if ((field->get_type()->is_set())
             && (get_true_type(((t_set *) field->get_type())->get_elem_type())->is_binary())) {
      indent(out) << "org.apache.thrift.TBaseHelper.toString(this." << field_name << "!!, sb)"
                  << endl;
    } else if ((field->get_type()->is_list())
               && (get_true_type(((t_list *) field->get_type())->get_elem_type())->is_binary())) {
      indent(out) << "org.apache.thrift.TBaseHelper.toString(this." << field_name << "!!, sb)"
                  << endl;
    } else {
      indent(out) << "sb.append(this." << field_name << ")" << endl;
    }

    if (can_be_null) {
      indent_down();
      indent(out) << "}" << endl;
    }
    indent(out) << "first = false" << endl;

    if (could_be_unset) {
      indent_down();
      indent(out) << "}" << endl;
    }
    first = false;
  }
  out << indent() << "sb.append(\")\");" << endl << indent() << "return sb.toString()" << endl;

  indent_down();
  indent(out) << "}" << endl << endl;
}

/**
 * Generates a static map with meta data to store information such as fieldID to
 * fieldName mapping
 *
 * @param tstruct The struct definition
 */
void t_kotlin_generator::generate_kotlin_meta_data_map(ostream &out, t_struct *tstruct) {
  const vector<t_field *> &fields = tstruct->get_members();
  vector<t_field *>::const_iterator f_iter;

  indent_up();
  // Static Map with fieldID -> org.apache.thrift.meta_data.FieldMetaData mappings
  indent(out)
          << "private val metaDataMap: Map<Fields, org.apache.thrift.meta_data.FieldMetaData> = mapOf(";
  indent_up();
  // Populate map
  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    out << endl;
    t_field *field = *f_iter;
    std::string field_name = field->get_name();
    indent(out) << "Fields." << constant_name(field_name)
                << " to org.apache.thrift.meta_data.FieldMetaData(\"" << field_name << "\", ";

    // Set field requirement type (required, optional, etc.)
    if (field->get_req() == t_field::T_REQUIRED) {
      out << "org.apache.thrift.TFieldRequirementType.REQUIRED, ";
    } else if (field->get_req() == t_field::T_OPTIONAL) {
      out << "org.apache.thrift.TFieldRequirementType.OPTIONAL, ";
    } else {
      out << "org.apache.thrift.TFieldRequirementType.DEFAULT, ";
    }

    // Create value meta data
    generate_field_value_meta_data(out, field->get_type());
    out << ")";
    if (f_iter + 1 != fields.end()) {
      out << ",";
    }
  }
  out << ")" << endl;
  indent_down();

  indent(out) << "init {" << endl;
  indent_up();
  indent(out) << "org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap("
              << type_name(tstruct) << "::class.java, metaDataMap)" << endl;
  scope_down(out);
  indent_down();
}

/**
 * Returns a string with the kotlin representation of the given thrift type
 * (e.g. for the type struct it returns "org.apache.thrift.protocol.TType.STRUCT")
 */
std::string t_kotlin_generator::get_kotlin_type_string(t_type *type) {
  if (type->is_list()) {
    return "org.apache.thrift.protocol.TType.LIST";
  } else if (type->is_map()) {
    return "org.apache.thrift.protocol.TType.MAP";
  } else if (type->is_set()) {
    return "org.apache.thrift.protocol.TType.SET";
  } else if (type->is_struct() || type->is_xception()) {
    return "org.apache.thrift.protocol.TType.STRUCT";
  } else if (type->is_enum()) {
    return "org.apache.thrift.protocol.TType.ENUM";
  } else if (type->is_typedef()) {
    return get_kotlin_type_string(((t_typedef *) type)->get_type());
  } else if (type->is_base_type()) {
    switch (((t_base_type *) type)->get_base()) {
      case t_base_type::TYPE_VOID:
        return "org.apache.thrift.protocol.TType.VOID";
        break;
      case t_base_type::TYPE_STRING:
        return "org.apache.thrift.protocol.TType.STRING";
        break;
      case t_base_type::TYPE_BOOL:
        return "org.apache.thrift.protocol.TType.BOOL";
        break;
      case t_base_type::TYPE_I8:
        return "org.apache.thrift.protocol.TType.BYTE";
        break;
      case t_base_type::TYPE_I16:
        return "org.apache.thrift.protocol.TType.I16";
        break;
      case t_base_type::TYPE_I32:
        return "org.apache.thrift.protocol.TType.I32";
        break;
      case t_base_type::TYPE_I64:
        return "org.apache.thrift.protocol.TType.I64";
        break;
      case t_base_type::TYPE_DOUBLE:
        return "org.apache.thrift.protocol.TType.DOUBLE";
        break;
      default:
        throw std::runtime_error("Unknown thrift type \"" + type->get_name()
                                 + "\" passed to t_kotlin_generator::get_kotlin_type_string!");
        return "Unknown thrift type \"" + type->get_name()
               + "\" passed to t_kotlin_generator::get_kotlin_type_string!";
        break; // This should never happen!
    }
  } else {
    throw std::runtime_error("Unknown thrift type \"" + type->get_name()
                             + "\" passed to t_kotlin_generator::get_kotlin_type_string!");
    // This should never happen!
  }
}

void t_kotlin_generator::generate_field_value_meta_data(std::ostream &out, t_type *type) {
  out << endl;
  indent_up();
  indent_up();
  if (type->is_struct() || type->is_xception()) {
    indent(out) << "org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType."
                   "STRUCT, " << type_name(type) << "::class.java";
  } else if (type->is_container()) {
    if (type->is_list()) {
      indent(out)
              << "org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, ";
      t_type *elem_type = ((t_list *) type)->get_elem_type();
      generate_field_value_meta_data(out, elem_type);
    } else if (type->is_set()) {
      indent(out)
              << "org.apache.thrift.meta_data.SetMetaData(org.apache.thrift.protocol.TType.SET, ";
      t_type *elem_type = ((t_set *) type)->get_elem_type();
      generate_field_value_meta_data(out, elem_type);
    } else { // map
      indent(out)
              << "org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, ";
      t_type *key_type = ((t_map *) type)->get_key_type();
      t_type *val_type = ((t_map *) type)->get_val_type();
      generate_field_value_meta_data(out, key_type);
      out << ", ";
      generate_field_value_meta_data(out, val_type);
    }
  } else if (type->is_enum()) {
    indent(out)
            << "org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, "
            << type_name(type) << "::class.java";
  } else {
    indent(out) << "org.apache.thrift.meta_data.FieldValueMetaData("
                << get_kotlin_type_string(type);
    if (type->is_typedef()) {
      indent(out) << ", \"" << ((t_typedef *) type)->get_symbolic() << "\"";
    } else if (type->is_binary()) {
      indent(out) << ", true";
    }
  }
  out << ")";
  indent_down();
  indent_down();
}

/**
 * Generates a thrift service. In C++, this comprises an entirely separate
 * header and source file. The header file defines the methods and includes
 * the data types defined in the main header file, and the implementation
 * file contains implementations of the basic printer and default interfaces.
 *
 * @param tservice The service definition
 */
void t_kotlin_generator::generate_service(t_service *tservice) {
  // Make output file
  string f_service_name = package_dir_ + "/" + make_valid_kotlin_filename(service_name_) + ".kt";
  f_service_.open(f_service_name.c_str());

  f_service_ << autogen_comment() << kotlin_package() << kotlin_suppressions();

  generate_java_generated_annotation(f_service_);
  f_service_ << "class " << service_name_ << " {" << endl << endl;
  indent_up();

  // Generate the three main parts of the service
  generate_service_interface(tservice);
  generate_service_async_interface(tservice);
  generate_service_client(tservice);
  generate_service_async_client(tservice);
  generate_service_server(tservice);
  generate_service_async_server(tservice);
  generate_service_helpers(tservice);

  indent_down();
  f_service_ << "}" << endl;
  f_service_.close();
}

/**
 * Generates a service interface definition.
 *
 * @param tservice The service to generate a header definition for
 */
void t_kotlin_generator::generate_service_interface(t_service *tservice) {
  string extends = "";
  string extends_iface = "";
  if (tservice->get_extends() != nullptr) {
    extends = type_name(tservice->get_extends());
    extends_iface = " : " + extends + ".Iface";
  }

  generate_java_doc(f_service_, tservice);
  f_service_ << indent() << "interface Iface" << extends_iface << " {" << endl << endl;
  indent_up();
  vector<t_function *> functions = tservice->get_functions();
  vector<t_function *>::iterator f_iter;
  for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {
    generate_java_doc(f_service_, *f_iter);
    indent(f_service_) << function_signature(*f_iter, false) << endl << endl;
  }
  indent_down();
  f_service_ << indent() << "}" << endl << endl;
}

void t_kotlin_generator::generate_service_async_interface(t_service *tservice) {
  string extends = "";
  string extends_iface = "";
  if (tservice->get_extends() != nullptr) {
    extends = type_name(tservice->get_extends());
    extends_iface = " : " + extends + " .AsyncIface";
  }

  f_service_ << indent() << "interface AsyncIface" << extends_iface << " {" << endl << endl;
  indent_up();
  vector<t_function *> functions = tservice->get_functions();
  vector<t_function *>::iterator f_iter;
  for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {
    indent(f_service_) << function_signature(*f_iter, true) << endl << endl;
  }
  indent_down();
  f_service_ << indent() << "}" << endl << endl;
}

/**
 * Generates structs for all the service args and return types
 *
 * @param tservice The service
 */
void t_kotlin_generator::generate_service_helpers(t_service *tservice) {
  vector<t_function *> functions = tservice->get_functions();
  vector<t_function *>::iterator f_iter;
  for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {
    t_struct *ts = (*f_iter)->get_arglist();
    generate_kotlin_struct_definition(f_service_, ts, false, true);
    generate_function_helpers(*f_iter);
  }
}

/**
 * Generates a service client definition.
 *
 * @param tservice The service to generate a server for.
 */
void t_kotlin_generator::generate_service_client(t_service *tservice) {
  string extends = "";
  string extends_client = "";
  if (tservice->get_extends() == nullptr) {
    extends_client = "org.apache.thrift.TServiceClient";
  } else {
    extends = type_name(tservice->get_extends());
    extends_client = extends + ".Client";
  }

  indent(f_service_) << "class Client(" << endl;
  indent(f_service_) << "    inputProtocol: org.apache.thrift.protocol.TProtocol," << endl;
  indent(f_service_) << "    outputProtocol : org.apache.thrift.protocol.TProtocol " << endl;
  indent(f_service_) << ") : org.apache.thrift.TServiceClient(inputProtocol, outputProtocol), Iface {" << endl;
  indent_up();
  indent(f_service_) << "constructor(prot:org.apache.thrift.protocol.TProtocol): this(prot, prot)"
                     << endl
                     << endl;

  indent(f_service_) << "companion object {" << endl;
  indent_up();
  indent(f_service_)
          << "class Factory : org.apache.thrift.TServiceClientFactory<Client> {"
          << endl;
  indent_up();
  indent(f_service_) << "override fun getClient(prot: org.apache.thrift.protocol.TProtocol):Client {"
                     << endl;
  indent_up();
  indent(f_service_) << "return Client(prot)" << endl;
  indent_down();
  indent(f_service_) << "}" << endl;
  indent(f_service_) << "override fun getClient(iprot: org.apache.thrift.protocol.TProtocol, "
                        "oprot: org.apache.thrift.protocol.TProtocol):Client {" << endl;
  indent_up();
  indent(f_service_) << "return Client(iprot, oprot)" << endl;
  indent_down();
  indent(f_service_) << "}" << endl;
  indent_down();
  indent(f_service_) << "}" << endl;
  scope_down(f_service_);

  // Generate client method implementations
  vector<t_function *> functions = tservice->get_functions();
  vector<t_function *>::const_iterator f_iter;
  for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {
    string funname = (*f_iter)->get_name();
    string sep = "_";
    string kotlinname = funname;
    sep = "";
    kotlinname = as_camel_case(funname);

    // Open function
    indent(f_service_) << function_signature(*f_iter, false, true) << endl;
    scope_up(f_service_);
    indent(f_service_) << "send" << sep << kotlinname << "(";

    // Get the struct of function call params
    t_struct *arg_struct = (*f_iter)->get_arglist();

    // Declare the function arguments
    const vector<t_field *> &fields = arg_struct->get_members();
    vector<t_field *>::const_iterator fld_iter;
    bool first = true;
    for (fld_iter = fields.begin(); fld_iter != fields.end(); ++fld_iter) {
      if (first) {
        first = false;
      } else {
        f_service_ << ", ";
      }
      f_service_ << (*fld_iter)->get_name();
    }
    f_service_ << ")" << endl;

    if (!(*f_iter)->is_oneway()) {
      f_service_ << indent();
      if (!(*f_iter)->get_returntype()->is_void()) {
        f_service_ << "return ";
      }
      f_service_ << "recv" << sep << kotlinname << "()" << endl;
    }
    scope_down(f_service_);
    f_service_ << endl;

    t_function send_function(g_type_void,
                             string("send") + sep + kotlinname,
                             (*f_iter)->get_arglist());

    string argsname = (*f_iter)->get_name() + "_args";

    // Open function
    indent(f_service_) << function_signature(&send_function) << endl;
    scope_up(f_service_);

    // Serialize the request
    indent(f_service_) << "val args:" << argsname << " = " << argsname << "()" << endl;

    for (fld_iter = fields.begin(); fld_iter != fields.end(); ++fld_iter) {
      indent(f_service_) << "args.set" << get_cap_name((*fld_iter)->get_name()) << "("
                         << (*fld_iter)->get_name() << ")" << endl;
    }

    const string sendBaseName = (*f_iter)->is_oneway() ? "sendBaseOneway" : "sendBase";
    indent(f_service_) << sendBaseName << "(\"" << funname << "\", args)" << endl;

    scope_down(f_service_);
    f_service_ << endl;

    if (!(*f_iter)->is_oneway()) {
      string resultname = (*f_iter)->get_name() + "_result";

      t_struct noargs(program_);
      t_function recv_function((*f_iter)->get_returntype(),
                               string("recv") + sep + kotlinname,
                               &noargs,
                               (*f_iter)->get_xceptions());
      // Open function
      indent(f_service_) << function_signature(&recv_function) << endl;
      scope_up(f_service_);

      f_service_ << indent() << "val result:" << resultname << " = " << resultname << "()" << endl
                 << indent() << "receiveBase(result, \"" << funname << "\")" << endl;

      // Careful, only return _result if not a void function
      if (!(*f_iter)->get_returntype()->is_void()) {
        f_service_ << indent() << "if (result." << generate_isset_check("success") << ") {" << endl
                   << indent() << "   return ";
        if ((*f_iter)->get_returntype()->is_binary()) {
          f_service_ << "java.nio.ByteBuffer.wrap(";
        }
        f_service_ << "result."
                   << ((*f_iter)->get_returntype()->is_bool() ? "is" : "get") << "Success()!!";
        if ((*f_iter)->get_returntype()->is_binary()) {
          f_service_ << ")";
        }
        f_service_ << endl << indent() << "}" << endl;
      }

      t_struct *xs = (*f_iter)->get_xceptions();
      const std::vector<t_field *> &xceptions = xs->get_members();
      vector<t_field *>::const_iterator x_iter;
      for (x_iter = xceptions.begin(); x_iter != xceptions.end(); ++x_iter) {
        f_service_ << indent() << "if (result." << generate_isset_check((*x_iter)->get_name())
                   << ") {" << endl
                   << indent() << "  throw result.get" << capitalize((*x_iter)->get_name()) << "()!!" << endl
                   << indent() << "}" << endl;
      }

      // If you get here it's an exception, unless a void function
      if ((*f_iter)->get_returntype()->is_void()) {
        indent(f_service_) << "return" << endl;
      } else {
        f_service_ << indent() << "throw "
                                  "org.apache.thrift.TApplicationException(org.apache.thrift."
                                  "TApplicationException.MISSING_RESULT, \""
                   << (*f_iter)->get_name() << " failed: unknown result\")" << endl;
      }

      // Close function
      scope_down(f_service_);
      f_service_ << endl;
    }
  }

  indent_down();
  indent(f_service_) << "}" << endl;
}

void t_kotlin_generator::generate_service_async_client(t_service *tservice) {
  indent(f_service_) << "class AsyncClient(" << endl;
  indent(f_service_) << "    inputProtocol: org.apache.thrift.protocol.TProtocol," << endl;
  indent(f_service_) << "    outputProtocol: org.apache.thrift.protocol.TProtocol " << endl;
  indent(f_service_) << ") : org.apache.thrift.TAsyncClient(inputProtocol, outputProtocol), AsyncIface {" << endl;
  indent_up();

  // Overload constructor
  indent(f_service_) << "constructor(prot: org.apache.thrift.protocol.TProtocol): this(prot, prot)" << endl << endl;

  indent(f_service_) << "companion object {" << endl;
  indent_up();
  indent(f_service_) << "class Factory : org.apache.thrift.TServiceClientFactory<AsyncClient> {" << endl;
  indent_up();
  indent(f_service_)
      << "override fun getClient(prot: org.apache.thrift.protocol.TProtocol):AsyncClient {" << endl;
  indent_up();
  indent(f_service_) << "return AsyncClient(prot)" << endl;
  indent_down();
  indent(f_service_) << "}" << endl;
  indent(f_service_) << "override fun getClient(iprot: org.apache.thrift.protocol.TProtocol, "
                        "oprot: org.apache.thrift.protocol.TProtocol):AsyncClient {"
                     << endl;
  indent_up();
  indent(f_service_) << "return AsyncClient(iprot, oprot)" << endl;
  indent_down();
  indent(f_service_) << "}" << endl;
  indent_down();
  indent(f_service_) << "}" << endl;
  scope_down(f_service_);

  // Generate client method implementations
  vector<t_function*> functions = tservice->get_functions();
  vector<t_function*>::const_iterator f_iter;
  for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {
    string funname = (*f_iter)->get_name();
    string sep = "_";
    string kotlinname = funname;
    sep = "";
    kotlinname = as_camel_case(funname);

    // Open function
    indent(f_service_) << function_signature(*f_iter, true, true) << endl;
    scope_up(f_service_);
    indent(f_service_) << "send" << sep << kotlinname << "(";

    // Get the struct of function call params
    t_struct* arg_struct = (*f_iter)->get_arglist();

    // Declare the function arguments
    const vector<t_field*>& fields = arg_struct->get_members();
    vector<t_field*>::const_iterator fld_iter;
    bool first = true;
    for (fld_iter = fields.begin(); fld_iter != fields.end(); ++fld_iter) {
      if (first) {
        first = false;
      } else {
        f_service_ << ", ";
      }
      f_service_ << (*fld_iter)->get_name();
    }
    f_service_ << ")" << endl;

    if (!(*f_iter)->is_oneway()) {
      f_service_ << indent();
      if (!(*f_iter)->get_returntype()->is_void()) {
        f_service_ << "return ";
      }
      f_service_ << "recv" << sep << kotlinname << "()" << endl;
    }
    scope_down(f_service_);
    f_service_ << endl;

    t_function send_function(g_type_void, string("send") + sep + kotlinname,
                             (*f_iter)->get_arglist());

    string argsname = (*f_iter)->get_name() + "_args";

    // Open function
    indent(f_service_) << function_signature(&send_function, true) << endl;
    scope_up(f_service_);

    // Serialize the request
    indent(f_service_) << "val args:" << argsname << " = " << argsname << "()" << endl;

    for (fld_iter = fields.begin(); fld_iter != fields.end(); ++fld_iter) {
      indent(f_service_) << "args.set" << get_cap_name((*fld_iter)->get_name()) << "("
                         << (*fld_iter)->get_name() << ")" << endl;
    }

    const string sendBaseName = (*f_iter)->is_oneway() ? "sendBaseOneway" : "sendBase";
    indent(f_service_) << sendBaseName << "(\"" << funname << "\", args)" << endl;

    scope_down(f_service_);
    f_service_ << endl;

    if (!(*f_iter)->is_oneway()) {
      string resultname = (*f_iter)->get_name() + "_result";

      t_struct noargs(program_);
      t_function recv_function((*f_iter)->get_returntype(), string("recv") + sep + kotlinname,
                               &noargs, (*f_iter)->get_xceptions());
      // Open function
      indent(f_service_) << function_signature(&recv_function, true) << endl;
      scope_up(f_service_);

      f_service_ << indent() << "val result:" << resultname << " = " << resultname << "()" << endl
                 << indent() << "receiveBase(result, \"" << funname << "\")" << endl;

      // Careful, only return _result if not a void function
      if (!(*f_iter)->get_returntype()->is_void()) {
        f_service_ << indent() << "if (result." << generate_isset_check("success") << ") {" << endl
                   << indent() << "  return ";
        if ((*f_iter)->get_returntype()->is_binary()) {
          f_service_ << "java.nio.ByteBuffer.wrap(";
        }
        f_service_ << "result." << ((*f_iter)->get_returntype()->is_bool() ? "is" : "get") << "Success()!!";
        if ((*f_iter)->get_returntype()->is_binary()) {
          f_service_ << ")";
        }
        f_service_ << endl << indent() << "}" << endl;
      }

      t_struct* xs = (*f_iter)->get_xceptions();
      const std::vector<t_field*>& xceptions = xs->get_members();
      vector<t_field*>::const_iterator x_iter;
      for (x_iter = xceptions.begin(); x_iter != xceptions.end(); ++x_iter) {
        f_service_ << indent() << "if (result." << generate_isset_check((*x_iter)->get_name()) << ") {" << endl
                   << indent() << "  throw result.get" << capitalize((*x_iter)->get_name()) << "()!!" << endl
                   << indent() << "}" << endl;
      }

      // If you get here it's an exception, unless a void function
      if ((*f_iter)->get_returntype()->is_void()) {
        indent(f_service_) << "return" << endl;
      } else {
        f_service_ << indent()
                   << "throw "
                      "org.apache.thrift.TApplicationException(org.apache.thrift."
                      "TApplicationException.MISSING_RESULT, \""
                   << (*f_iter)->get_name() << " failed: unknown result\")" << endl;
      }

      // Close function
      scope_down(f_service_);
      f_service_ << endl;
    }
  }

  indent_down();
  indent(f_service_) << "}" << endl;
}

/**
 * Generates a service server definition.
 *
 * @param tservice The service to generate a server for.
 */
void t_kotlin_generator::generate_service_server(t_service *tservice) {
  // Generate the dispatch methods
  vector<t_function *> functions = tservice->get_functions();
  vector<t_function *>::iterator f_iter;

  // Extends stuff
  string extends = "";
  string extends_processor = "";
  if (tservice->get_extends() == nullptr) {
    extends_processor = "org.apache.thrift.TBaseProcessor<Iface>(iface, processMap)";
  } else {
    extends = type_name(tservice->get_extends());
    extends_processor = extends + ".Processor<I>";
  }

  // Generate the header portion
  indent(f_service_) << "class Processor(" << endl;
  indent_up();
  indent(f_service_) << "iface:Iface," << endl;
  indent(f_service_) << "processMap:Map<String, org.apache.thrift.ProcessFunction<Iface, out org.apache.thrift.TBase<*, *>>>" << endl;
  indent_down();
  indent(f_service_) << "): " << extends_processor << ", org.apache.thrift.TProcessor {" << endl;
  indent_up();

  indent(f_service_) << "constructor(iface:Iface): this(iface, getProcessMap(mutableMapOf<String, "
                     << "org.apache.thrift.ProcessFunction<Iface, out org.apache.thrift.TBase<*, *>>>()))"
                     << endl << endl;

  indent(f_service_) << "companion object {" << endl;
  indent_up();
  indent(f_service_) << "fun getProcessMap(processMap: MutableMap<String, "
                        "org.apache.thrift.ProcessFunction<Iface, out org.apache.thrift.TBase<*, *>>>): "
                        "Map<String, org.apache.thrift.ProcessFunction<Iface, "
                        "out org.apache.thrift.TBase<*, *>>> {" << endl;
  indent_up();
  for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {
    indent(f_service_) << "processMap[\"" << (*f_iter)->get_name() << "\"] = "
                       << (*f_iter)->get_name() << "()" << endl;
  }
  indent(f_service_) << "return processMap.toMap()" << endl;
  indent_down();
  indent(f_service_) << "}" << endl << endl;

  // Generate the process subfunctions
  for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {
    generate_process_function(tservice, *f_iter, false);
  }

  indent_down();
  indent(f_service_) << "}" << endl;
  indent_down();
  indent(f_service_) << "}" << endl << endl;
}

/**
 * Generates a service server definition.
 *
 * @param tservice The service to generate a server for.
 */
void t_kotlin_generator::generate_service_async_server(t_service *tservice) {
  // Generate the dispatch methods
  vector<t_function *> functions = tservice->get_functions();
  vector<t_function *>::iterator f_iter;

  // Extends stuff
  string extends = "";
  string extends_processor = "";
  if (tservice->get_extends() == nullptr) {
    extends_processor = "org.apache.thrift.TAsyncBaseProcessor<AsyncIface>(iface, processMap)";
  } else {
    extends = type_name(tservice->get_extends());
    extends_processor = extends + ".Processor<AsyncIface>";
  }

  // Generate the header portion
  indent(f_service_) << "class AsyncProcessor(" << endl;
  indent_up();
  indent(f_service_) << "iface:AsyncIface," << endl;
  indent(f_service_) << "processMap:Map<String, org.apache.thrift.AsyncProcessFunction<AsyncIface, out org.apache.thrift.TBase<*, *>>>" << endl;
  indent_down();
  indent(f_service_) << "): " << extends_processor << ", org.apache.thrift.TProcessor {" << endl;
  indent_up();

  indent(f_service_) << "constructor(iface:AsyncIface): this(iface, getProcessMap(mutableMapOf<String, "
                     << "org.apache.thrift.AsyncProcessFunction<AsyncIface, out org.apache.thrift.TBase<*, *>>>()))"
                     << endl << endl;

  indent(f_service_) << "companion object {" << endl;
  indent_up();
  indent(f_service_) << "fun getProcessMap(processMap: MutableMap<String, "
                        "org.apache.thrift.AsyncProcessFunction<AsyncIface, out org.apache.thrift.TBase<*, *>>>): "
                        "Map<String, org.apache.thrift.AsyncProcessFunction<AsyncIface, "
                        "out org.apache.thrift.TBase<*, *>>> {" << endl;
  indent_up();
  for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {
    indent(f_service_) << "processMap[\"" << (*f_iter)->get_name() << "\"] = "
                       << (*f_iter)->get_name() << "()" << endl;
  }
  indent(f_service_) << "return processMap.toMap()" << endl;
  indent_down();
  indent(f_service_) << "}" << endl << endl;

  // Generate the process subfunctions
  for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {
    generate_process_function(tservice, *f_iter, true);
  }

  indent_down();
  indent(f_service_) << "}" << endl;
  indent_down();
  indent(f_service_) << "}" << endl << endl;
}

/**
 * Generates a struct and helpers for a function.
 *
 * @param tfunction The function
 */
void t_kotlin_generator::generate_function_helpers(t_function *tfunction) {
  if (tfunction->is_oneway()) {
    return;
  }

  t_struct result(program_, tfunction->get_name() + "_result");
  t_field success(tfunction->get_returntype(), "success", 0);
  if (!tfunction->get_returntype()->is_void()) {
    result.append(&success);
  }

  t_struct *xs = tfunction->get_xceptions();
  const vector<t_field *> &fields = xs->get_members();
  vector<t_field *>::const_iterator f_iter;
  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    result.append(*f_iter);
  }

  generate_kotlin_struct_definition(f_service_, &result, false, true, true);
}

/**
 * Generates a process function definition.
 *
 * @param tfunction The function to write a dispatcher for
 */
void t_kotlin_generator::generate_process_function(t_service *tservice, t_function *tfunction, bool async) {
  string argsname = tfunction->get_name() + "_args";
  string resultname = tfunction->get_name() + "_result";
  string iface = async ? "AsyncIface" : "Iface";
  string processorBase = async ? "AsyncProcessFunction" : "ProcessFunction";
  string suspend = async ? "suspend " : "";
  if (tfunction->is_oneway()) {
    resultname = "org.apache.thrift.TBase<*, *>?";
  }

  (void) tservice;
  // Companion class
  indent(f_service_) << "class " << tfunction->get_name()
                     << ": org.apache.thrift." << processorBase << "<" << iface << ", "
                     << argsname << ">(\"" << tfunction->get_name() << "\") {" << endl;
  indent_up();

  indent(f_service_) << "override val emptyArgsInstance: " << argsname << endl;
  indent_up();
  indent(f_service_) << "get() = " << argsname << "()" << endl;
  indent_down();


  indent(f_service_) << "override val isOneway: Boolean" << endl;
  indent_up();
  indent(f_service_) << "get() = " << ((tfunction->is_oneway()) ? "true" : "false") << endl;
  indent_down();

  indent(f_service_) << "override fun rethrowUnhandledExceptions(): Boolean {" << endl;
  indent(f_service_) << "  return true" << endl;
  indent(f_service_) << "}" << endl << endl;

  indent(f_service_) << "@Throws(org.apache.thrift.TException::class)" << endl;
  indent(f_service_) << "override " << suspend << "fun getResult(iface: " << iface << ", args:" << argsname
                     << "): " << resultname << " {" << endl;
  indent_up();
  if (!tfunction->is_oneway()) {
    indent(f_service_) << "val result: " << resultname << " = " << resultname << "()" << endl;
  }

  t_struct *xs = tfunction->get_xceptions();
  const std::vector<t_field *> &xceptions = xs->get_members();
  vector<t_field *>::const_iterator x_iter;

  // Try block for a function with exceptions
  if (xceptions.size() > 0) {
    f_service_ << indent() << "try {" << endl;
    indent_up();
  }

  // Generate the function call
  t_struct *arg_struct = tfunction->get_arglist();
  const std::vector<t_field *> &fields = arg_struct->get_members();
  vector<t_field *>::const_iterator f_iter;
  f_service_ << indent();

  if (!tfunction->is_oneway() && !tfunction->get_returntype()->is_void()) {
    f_service_ << "result.setSuccess(";
  }
  f_service_ << "iface." << get_rpc_method_name(tfunction->get_name()) << "(";
  bool first = true;
  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    if (first) {
      first = false;
    } else {
      f_service_ << ", ";
    }
    t_type* type = (*f_iter)->get_type();
    if ((*f_iter)->get_type()->get_true_type()->is_binary()) {
      f_service_ << "java.nio.ByteBuffer.wrap(";
    }
    f_service_ << "args." << ((*f_iter)->get_type()->is_bool() ? "is" : "get") << capitalize((*f_iter)->get_name()) << "()";
    if ((*f_iter)->get_type()->get_true_type()->is_binary()) {
      f_service_ << ")";
    }
  }
  if (!tfunction->is_oneway() && !tfunction->get_returntype()->is_void()) {
    f_service_ << ")";
  }
  f_service_ << ")" << endl;

  // Set isset on success field
  if (!tfunction->is_oneway() && !tfunction->get_returntype()->is_void()
      && !type_can_be_null(tfunction->get_returntype())) {
    indent(f_service_) << "result.set" << get_cap_name("success") << get_cap_name("isSet")
                       << "(true)" << endl;
  }

  if (!tfunction->is_oneway() && xceptions.size() > 0) {
    indent_down();
    f_service_ << indent() << "}";
    for (x_iter = xceptions.begin(); x_iter != xceptions.end(); ++x_iter) {
      f_service_ << " catch (" << (*x_iter)->get_name() << ": "
                 << type_name((*x_iter)->get_type(), false, false) << ") {" << endl;
      if (!tfunction->is_oneway()) {
        indent_up();
        f_service_ << indent() << "result.set" << capitalize((*x_iter)->get_name()) << "("
                   << (*x_iter)->get_name() << ")" << endl;
        indent_down();
        f_service_ << indent() << "}";
      } else {
        f_service_ << "}";
      }
    }
    f_service_ << endl;
  }

  if (tfunction->is_oneway()) {
    indent(f_service_) << "return null" << endl;
  } else {
    indent(f_service_) << "return result" << endl;
  }
  indent_down();
  indent(f_service_) << "}";

  // Close function
  f_service_ << endl;

  // Close class
  indent_down();
  f_service_ << indent() << "}" << endl;
}

/**
 * Deserializes a field of any type.
 *
 * @param tfield The field
 * @param prefix The variable name or container for this field
 */
void t_kotlin_generator::generate_deserialize_field(ostream &out,
                                                    t_field *tfield,
                                                    const string &prefix,
                                                    bool has_metadata) {
  t_type *type = get_true_type(tfield->get_type());

  if (type->is_void()) {
    throw "CANNOT GENERATE DESERIALIZE CODE FOR void TYPE: " + prefix + tfield->get_name();
  }

  string name = prefix + tfield->get_name();

  if (type->is_struct() || type->is_xception()) {
    generate_deserialize_struct(out, (t_struct *) type, name);
  } else if (type->is_container()) {
    generate_deserialize_container(out, type, name, has_metadata);
  } else if (type->is_base_type()) {
    indent(out) << name << " = iprot.";

    t_base_type::t_base tbase = ((t_base_type *) type)->get_base();
    switch (tbase) {
      case t_base_type::TYPE_VOID:
        throw "compiler error: cannot serialize void field in a struct: " + name;
        break;
      case t_base_type::TYPE_STRING:
        if (type->is_binary()) {
          out << "readBinary()";
        } else {
          out << "readString()";
        }
        break;
      case t_base_type::TYPE_BOOL:
        out << "readBool()";
        break;
      case t_base_type::TYPE_I8:
        out << "readByte()";
        break;
      case t_base_type::TYPE_I16:
        out << "readI16()";
        break;
      case t_base_type::TYPE_I32:
        out << "readI32()";
        break;
      case t_base_type::TYPE_I64:
        out << "readI64()";
        break;
      case t_base_type::TYPE_DOUBLE:
        out << "readDouble()";
        break;
      default:
        throw "compiler error: no kotlin name for base type " + t_base_type::t_base_name(tbase);
    }
    out << endl;
  } else if (type->is_enum()) {
    indent(out) << name << " = "
                << type_name(tfield->get_type(), true, false, false, true)
                   + ".findByValue(iprot.readI32())" << endl;
  } else {
    printf("DO NOT KNOW HOW TO DESERIALIZE FIELD '%s' TYPE '%s'\n",
           tfield->get_name().c_str(),
           type_name(type).c_str());
  }
}

/**
 * Generates an unserializer for a struct, invokes read()
 */
void t_kotlin_generator::generate_deserialize_struct(ostream &out,
                                                     t_struct *tstruct,
                                                     const string &prefix) {

  indent(out) << prefix << " = " << type_name(tstruct) << "()" << endl;
  indent(out) << prefix << kotlin_nullable_annotation() << ".read(iprot)" << endl;
}

/**
 * Deserializes a container by reading its size and then iterating
 */
void t_kotlin_generator::generate_deserialize_container(ostream &out,
                                                        t_type *ttype,
                                                        const string &prefix,
                                                        bool has_metadata) {
  string obj;

  if (ttype->is_map()) {
    obj = tmp("_map");
  } else if (ttype->is_set()) {
    obj = tmp("_set");
  } else if (ttype->is_list()) {
    obj = tmp("_list");
  }

  if (has_metadata) {
    // Declare variables, read header
    if (ttype->is_map()) {
      indent(out) << "val " << obj << ":org.apache.thrift.protocol.TMap = iprot.readMapBegin()"
                  << endl;
    } else if (ttype->is_set()) {
      indent(out) << "val " << obj << ":org.apache.thrift.protocol.TSet = iprot.readSetBegin()"
                  << endl;
    } else if (ttype->is_list()) {
      indent(out) << "val " << obj << ":org.apache.thrift.protocol.TList = iprot.readListBegin()"
                  << endl;
    }
  } else {
    // Declare variables, read header
    if (ttype->is_map()) {
      indent(out) << "val " << obj << ":org.apache.thrift.protocol.TMap "
                  << " = iprot.readMapBegin("
                  << type_to_enum(((t_map *) ttype)->get_key_type()) << ", "
                  << type_to_enum(((t_map *) ttype)->get_val_type()) << ") " << endl;
    } else if (ttype->is_set()) {
      indent(out) << "val " << obj << ":org.apache.thrift.protocol.TSet "
                  << " = iprot.readSetBegin("
                  << type_to_enum(((t_set *) ttype)->get_elem_type()) << ")"
                  << endl;
    } else if (ttype->is_list()) {
      indent(out) << "val " << obj << ":org.apache.thrift.protocol.TList "
                  << " = iprot.readListBegin("
                  << type_to_enum(((t_list *) ttype)->get_elem_type()) << ")"
                  << endl;
    }
  }

  if (is_enum_set(ttype)) {
    out << indent() << prefix << " = " << type_name(ttype, false, true, true) << ".noneOf";
  } else {
    out << indent() << prefix << " = " << type_name(ttype, false, true);
  }

  // construct the collection correctly i.e. with appropriate size/type
  if (is_enum_set(ttype) || is_enum_map(ttype)) {
    out << "(" << inner_enum_type_name(ttype) << ")" << endl;
  } else if (ttype->is_map() || ttype->is_set()) {
    // TreeSet and TreeMap don't have any constructor which takes a capacity as an argument
    out << "()" << endl;
  } else {
    out << "(" << (ttype->is_list() ? "" : "2*") << obj << ".size"
        << ")" << endl;
  }

  if (ttype->is_map()) {
    generate_deserialize_map_element(out, (t_map *) ttype, prefix, obj, has_metadata);
  } else if (ttype->is_set()) {
    generate_deserialize_set_element(out, (t_set *) ttype, prefix, obj, has_metadata);
  } else if (ttype->is_list()) {
    generate_deserialize_list_element(out, (t_list *) ttype, prefix, obj, has_metadata);
  }

  scope_down(out);

  if (has_metadata) {
    // Read container end
    if (ttype->is_map()) {
      indent(out) << "iprot.readMapEnd()" << endl;
    } else if (ttype->is_set()) {
      indent(out) << "iprot.readSetEnd()" << endl;
    } else if (ttype->is_list()) {
      indent(out) << "iprot.readListEnd()" << endl;
    }
  }
}

/**
 * Generates code to deserialize a map
 */
void t_kotlin_generator::generate_deserialize_map_element(ostream &out,
                                                          t_map *tmap,
                                                          const string &prefix,
                                                          const string &obj,
                                                          bool has_metadata) {
  string key = tmp("_key");
  string val = tmp("_val");
  t_field fkey(tmap->get_key_type(), key);
  t_field fval(tmap->get_val_type(), val);

  indent(out) << "var " << declare_field(&fkey, false, true) << endl;
  indent(out) << "var " << declare_field(&fval, false, true) << endl;

  // For loop iterates over elements
  string i = tmp("_i");
  indent(out) << "for (" << i << " in 0.." << obj << ".size)" << endl;

  scope_up(out);

  generate_deserialize_field(out, &fkey, "", has_metadata);
  generate_deserialize_field(out, &fval, "", has_metadata);

  if (get_true_type(fkey.get_type())->is_enum()) {
    indent(out) << "if (" << key << " != null)" << endl;
    scope_up(out);
  }

  indent(out) << prefix << "?.put(" << key << "!!, " << val << "!!)" << endl;

  if (get_true_type(fkey.get_type())->is_enum()) {
    scope_down(out);
  }

  if (!get_true_type(fkey.get_type())->is_base_type()) {
    indent(out) << key << " = null" << endl;
  }

  if (!get_true_type(fval.get_type())->is_base_type()) {
    indent(out) << val << " = null" << endl;
  }
}

/**
 * Deserializes a set element
 */
void t_kotlin_generator::generate_deserialize_set_element(ostream &out,
                                                          t_set *tset,
                                                          const string &prefix,
                                                          const string &obj,
                                                          bool has_metadata) {
  string elem = tmp("_elem");
  t_field felem(tset->get_elem_type(), elem);

  indent(out) << "var " << declare_field(&felem, false, true) << endl;

  // For loop iterates over elements
  string i = tmp("_i");
  indent(out) << "for (" << i << " in 0.." << obj << ".size)" << endl;
  scope_up(out);

  generate_deserialize_field(out, &felem, "", has_metadata);

  if (get_true_type(felem.get_type())->is_enum()) {
    indent(out) << "if (" << elem << " != null)" << endl;
    scope_up(out);
  }

  indent(out) << prefix << "?.add(" << elem << "!!)" << endl;

  if (get_true_type(felem.get_type())->is_enum()) {
    scope_down(out);
  }

  if (!get_true_type(felem.get_type())->is_base_type()) {
    indent(out) << elem << " = null" << endl;
  }
}

/**
 * Deserializes a list element
 */
void t_kotlin_generator::generate_deserialize_list_element(ostream &out,
                                                           t_list *tlist,
                                                           const string &prefix,
                                                           const string &obj,
                                                           bool has_metadata) {
  string elem = tmp("_elem");
  t_field felem(tlist->get_elem_type(), elem);

  indent(out) << "var " << declare_field(&felem, false, true) << endl;

  // For loop iterates over elements
  string i = tmp("_i");
  indent(out) << "for (" << i << " in 0.." << obj << ".size)" << endl;
  scope_up(out);

  generate_deserialize_field(out, &felem, "", has_metadata);

  if (get_true_type(felem.get_type())->is_enum()) {
    indent(out) << "if (" << elem << " != null)" << endl;
    scope_up(out);
  }

  indent(out) << prefix << "?.add(" << elem << "!!)" << endl;

  if (get_true_type(felem.get_type())->is_enum()) {
    scope_down(out);
  }

  if (!get_true_type(felem.get_type())->is_base_type()) {
    indent(out) << elem << " = null" << endl;
  }
}

/**
 * Serializes a field of any type.
 *
 * @param tfield The field to serialize
 * @param prefix Name to prepend to field name
 */
void t_kotlin_generator::generate_serialize_field(ostream &out,
                                                  t_field *tfield,
                                                  const string &prefix,
                                                  bool has_metadata) {
  t_type *type = get_true_type(tfield->get_type());

  // Do nothing for void types
  if (type->is_void()) {
    throw "CANNOT GENERATE SERIALIZE CODE FOR void TYPE: " + prefix + tfield->get_name();
  }

  if (type->is_struct() || type->is_xception()) {
    generate_serialize_struct(out, (t_struct *) type,
                              prefix + tfield->get_name() + kotlin_nullable_annotation());
  } else if (type->is_container()) {
    generate_serialize_container(out, type, prefix + tfield->get_name() + "!!", has_metadata);
  } else if (type->is_enum()) {
    indent(out) << "oprot.writeI32(" << prefix + tfield->get_name() << "!!.value)" << endl;
  } else if (type->is_base_type()) {
    string name = prefix + tfield->get_name();
    indent(out) << "oprot.";

    if (type->is_base_type()) {
      t_base_type::t_base tbase = ((t_base_type *) type)->get_base();
      switch (tbase) {
        case t_base_type::TYPE_VOID:
          throw "compiler error: cannot serialize void field in a struct: " + name;
          break;
        case t_base_type::TYPE_STRING:
          if (type->is_binary()) {
            out << "writeBinary(" << name << ")";
          } else {
            out << "writeString(" << name << ")";
          }
          break;
        case t_base_type::TYPE_BOOL:
          out << "writeBool(" << name << ")";
          break;
        case t_base_type::TYPE_I8:
          out << "writeByte(" << name << ")";
          break;
        case t_base_type::TYPE_I16:
          out << "writeI16(" << name << ")";
          break;
        case t_base_type::TYPE_I32:
          out << "writeI32(" << name << ")";
          break;
        case t_base_type::TYPE_I64:
          out << "writeI64(" << name << ")";
          break;
        case t_base_type::TYPE_DOUBLE:
          out << "writeDouble(" << name << ")";
          break;
        default:
          throw "compiler error: no kotlin name for base type " + t_base_type::t_base_name(tbase);
      }
    } else if (type->is_enum()) {
      out << "writeI32(struct." << name << ")";
    }
    out << endl;
  } else {
    printf("DO NOT KNOW HOW TO SERIALIZE FIELD '%s%s' TYPE '%s'\n",
           prefix.c_str(),
           tfield->get_name().c_str(),
           type_name(type).c_str());
  }
}

/**
 * Serializes all the members of a struct.
 *
 * @param tstruct The struct to serialize
 * @param prefix  String prefix to attach to all fields
 */
void t_kotlin_generator::generate_serialize_struct(ostream &out, t_struct *tstruct, const string &prefix) {
  (void) tstruct;
  out << indent() << prefix << ".write(oprot);" << endl;
}

/**
 * Serializes a container by writing its size then the elements.
 *
 * @param ttype  The type of container
 * @param prefix String prefix for fields
 */
void t_kotlin_generator::generate_serialize_container(ostream &out,
                                                      t_type *ttype,
                                                      const string &prefix,
                                                      bool has_metadata) {
  if (has_metadata) {
    if (ttype->is_map()) {
      indent(out) << "oprot.writeMapBegin(org.apache.thrift.protocol.TMap("
                  << type_to_enum(((t_map *) ttype)->get_key_type()) << ", "
                  << type_to_enum(((t_map *) ttype)->get_val_type()) << ", " << prefix << ".size))"
                  << endl;
    } else if (ttype->is_set()) {
      indent(out) << "oprot.writeSetBegin(org.apache.thrift.protocol.TSet("
                  << type_to_enum(((t_set *) ttype)->get_elem_type()) << ", " << prefix
                  << ".size))" << endl;
    } else if (ttype->is_list()) {
      indent(out) << "oprot.writeListBegin(org.apache.thrift.protocol.TList("
                  << type_to_enum(((t_list *) ttype)->get_elem_type()) << ", " << prefix
                  << ".size))" << endl;
    }
  } else {
    indent(out) << "oprot.writeI32(" << prefix << ".size)" << endl;
  }

  string iter = tmp("_iter");
  if (ttype->is_map()) {
    indent(out) << "for (" << iter << ":MutableMap.MutableEntry<"
                << type_name(((t_map *) ttype)->get_key_type(), true, false)
                << ", " << type_name(((t_map *) ttype)->get_val_type(), true, false) << "> in "
                << prefix << ".entries)";
  } else if (ttype->is_set()) {
    indent(out) << "for (" << iter << ":" << type_name(((t_set *) ttype)->get_elem_type()) << " in "
                << prefix << ")";
  } else if (ttype->is_list()) {
    indent(out) << "for (" << iter << ":" << type_name(((t_list *) ttype)->get_elem_type())
                << " in " << prefix << ")";
  }

  out << endl;
  scope_up(out);
  if (ttype->is_map()) {
    generate_serialize_map_element(out, (t_map *) ttype, iter, prefix, has_metadata);
  } else if (ttype->is_set()) {
    generate_serialize_set_element(out, (t_set *) ttype, iter, has_metadata);
  } else if (ttype->is_list()) {
    generate_serialize_list_element(out, (t_list *) ttype, iter, has_metadata);
  }
  scope_down(out);

  if (has_metadata) {
    if (ttype->is_map()) {
      indent(out) << "oprot.writeMapEnd()" << endl;
    } else if (ttype->is_set()) {
      indent(out) << "oprot.writeSetEnd()" << endl;
    } else if (ttype->is_list()) {
      indent(out) << "oprot.writeListEnd()" << endl;
    }
  }
}

/**
 * Serializes the members of a map.
 */
void t_kotlin_generator::generate_serialize_map_element(ostream &out,
                                                        t_map *tmap,
                                                        const string &iter,
                                                        const string &map,
                                                        bool has_metadata) {
  (void) map;
  t_field kfield(tmap->get_key_type(), iter + ".key");
  generate_serialize_field(out, &kfield, "", has_metadata);
  t_field vfield(tmap->get_val_type(), iter + ".value");
  generate_serialize_field(out, &vfield, "", has_metadata);
}

/**
 * Serializes the members of a set.
 */
void t_kotlin_generator::generate_serialize_set_element(ostream &out,
                                                        t_set *tset,
                                                        string iter,
                                                        bool has_metadata) {
  t_field efield(tset->get_elem_type(), iter);
  generate_serialize_field(out, &efield, "", has_metadata);
}

/**
 * Serializes the members of a list.
 */
void t_kotlin_generator::generate_serialize_list_element(ostream &out,
                                                         t_list *tlist,
                                                         string iter,
                                                         bool has_metadata) {
  t_field efield(tlist->get_elem_type(), iter);
  generate_serialize_field(out, &efield, "", has_metadata);
}

/**
 * Returns a kotlin type name
 *
 * @param ttype The type
 * @param container Is the type going inside a container?
 * @return type name, i.e. java.util.HashMap<Key,Value>
 */
string t_kotlin_generator::type_name(t_type *ttype,
                                     bool in_container,
                                     bool in_init,
                                     bool skip_generic,
                                     bool force_namespace,
                                     bool generic_generic) {
  // In kotlin typedefs are just resolved to their real type
  ttype = get_true_type(ttype);
  string prefix;

  if (ttype->is_base_type()) {
    return base_type_name((t_base_type *) ttype, in_container);
  } else if (ttype->is_map()) {
    t_map *tmap = (t_map *) ttype;
    if (is_enum_map(tmap)) {
      prefix = "java.util.EnumMap";
    } else {
      prefix = "HashMap";
    }
    return prefix + (skip_generic ? (generic_generic ? "<*, *>" : "") : "<" + type_name(tmap->get_key_type(), true) + ","
                                         + type_name(tmap->get_val_type(), true) + ">");
  } else if (ttype->is_set()) {
    t_set *tset = (t_set *) ttype;
    if (is_enum_set(tset)) {
      prefix = "EnumSet";
    } else {
      prefix = "HashSet";
    }
    return prefix + (skip_generic ? (generic_generic ? "<*>" : "") : "<" + type_name(tset->get_elem_type(), true) + ">");
  } else if (ttype->is_list()) {
    t_list *tlist = (t_list *) ttype;
    return "ArrayList" + (skip_generic ? (generic_generic ? "<*>" : "") : "<" + type_name(tlist->get_elem_type(), true) + ">");
  }

  // Check for namespacing
  t_program *program = ttype->get_program();
  if ((program != nullptr) && ((program != program_) || force_namespace)) {
    string package = program->get_namespace("kotlin");
    if (!package.empty()) {
      return package + "." + ttype->get_name();
    }
  }

  return ttype->get_name();
}

/**
 * Returns the kotlin type that corresponds to the thrift type.
 *
 * @param tbase The base type
 * @param container Is it going in a kotlin container?
 */
string t_kotlin_generator::base_type_name(t_base_type *type, bool in_container) {
  t_base_type::t_base tbase = type->get_base();

  switch (tbase) {
    case t_base_type::TYPE_VOID:
      return (in_container ? "Void" : "void");
    case t_base_type::TYPE_STRING:
      if (type->is_binary()) {
        return "java.nio.ByteBuffer";
      } else {
        return "String";
      }
    case t_base_type::TYPE_BOOL:
      return "Boolean";
    case t_base_type::TYPE_I8:
      return "Byte";
    case t_base_type::TYPE_I16:
      return "Short";
    case t_base_type::TYPE_I32:
      return "Int";
    case t_base_type::TYPE_I64:
      return "Long";
    case t_base_type::TYPE_DOUBLE:
      return "Double";
    default:
      throw "compiler error: no kotlin name for base type " + t_base_type::t_base_name(tbase);
  }
}

/**
 * Declares a field, which may include initialization as necessary.
 *
 * @param tfield The field
 * @param init Whether to initialize the field
 */
string t_kotlin_generator::declare_field(t_field *tfield, bool comment, bool last) {
  string result;
  t_type *ttype = get_true_type(tfield->get_type());
  result += tfield->get_name() + ": " + type_name(tfield->get_type());
  if (type_can_be_null(ttype)) {
    result += "?";
  }
  if (ttype->is_base_type() && tfield->get_value() != nullptr) {
    std::ofstream dummy;
    result += " = " + render_const_value(dummy, ttype, tfield->get_value());
  } else if (ttype->is_base_type()) {
    t_base_type::t_base tbase = ((t_base_type *) ttype)->get_base();
    switch (tbase) {
      case t_base_type::TYPE_VOID:
        throw "NO T_VOID CONSTRUCT";
      case t_base_type::TYPE_STRING:
        result += " = null";
        break;
      case t_base_type::TYPE_BOOL:
        result += " = false";
        break;
      case t_base_type::TYPE_I8:
      case t_base_type::TYPE_I16:
      case t_base_type::TYPE_I32:
      case t_base_type::TYPE_I64:
        result += " = 0";
        break;
      case t_base_type::TYPE_DOUBLE:
        result += " = 0.0";
        break;
    }
  } else if (ttype->is_enum() || is_enum_map(ttype)) {
    result += " = null";
  } else {
    result += " = " + type_name(ttype, false, true) + "()";
  }
  if (!last) {
    result += ",";
  }
  if (comment) {
    result += " // ";
    if (tfield->get_req() == t_field::T_OPTIONAL) {
      result += "optional";
    } else {
      result += "required";
    }
  }
  return result;
}

/**
 * Renders a function signature of the form '@Throws(org.apache.thrift.TException::class, <others>)\n<suspend> fun <name>(<args>):<returnType>'
 *
 * @param tfunction Function definition
 * @return String of rendered function definition
 */
string t_kotlin_generator::function_signature(t_function* tfunction,
                                              bool suspend,
                                              bool has_override,
                                              string visibility) {
  t_type *ttype = tfunction->get_returntype();
  std::string fn_name = get_rpc_method_name(tfunction->get_name());
  std::string result = "@Throws(org.apache.thrift.TException::class";
  t_struct* xs = tfunction->get_xceptions();
  const std::vector<t_field*>& xceptions = xs->get_members();
  vector<t_field*>::const_iterator x_iter;
  for (x_iter = xceptions.begin(); x_iter != xceptions.end(); ++x_iter) {
    result += ", " + type_name((*x_iter)->get_type()) + "::class";
  }
  result += ")" + endl;
  result += indent() +  visibility + (has_override ? "override " : "") + (suspend ? "suspend " : "")
            + "fun " + fn_name + "(" + argument_list(tfunction->get_arglist()) + ")";
  if(!tfunction->get_returntype()->is_void()) {
     result += ":" + type_name(ttype);
  }
  return result;
}

/**
 * Renders a comma separated field list, with type names
 */
string t_kotlin_generator::argument_list(t_struct *tstruct, bool include_types) {
  string result = "";

  const vector<t_field *> &fields = tstruct->get_members();
  vector<t_field *>::const_iterator f_iter;
  bool first = true;
  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    if (first) {
      first = false;
    } else {
      result += ", ";
    }
    result += (*f_iter)->get_name();
    if (include_types) {
      result += ":" + type_name((*f_iter)->get_type());
      // Arguments can be null. To be compatible with other languages we have to use ? in kotlin.
      // Strings are nullable.
      t_type* ttype = (*f_iter)->get_type()->get_true_type();
      if(!ttype->is_base_type() || ttype->is_string()) {
        result += "?";
      }
    }
  }
  return result;
}

/**
 * Converts the parse type to a kotlin enum string for the given type.
 */
string t_kotlin_generator::type_to_enum(t_type *type) {
  type = get_true_type(type);

  if (type->is_base_type()) {
    t_base_type::t_base tbase = ((t_base_type *) type)->get_base();
    switch (tbase) {
      case t_base_type::TYPE_VOID:
        throw "NO T_VOID CONSTRUCT";
      case t_base_type::TYPE_STRING:
        return "org.apache.thrift.protocol.TType.STRING";
      case t_base_type::TYPE_BOOL:
        return "org.apache.thrift.protocol.TType.BOOL";
      case t_base_type::TYPE_I8:
        return "org.apache.thrift.protocol.TType.BYTE";
      case t_base_type::TYPE_I16:
        return "org.apache.thrift.protocol.TType.I16";
      case t_base_type::TYPE_I32:
        return "org.apache.thrift.protocol.TType.I32";
      case t_base_type::TYPE_I64:
        return "org.apache.thrift.protocol.TType.I64";
      case t_base_type::TYPE_DOUBLE:
        return "org.apache.thrift.protocol.TType.DOUBLE";
    }
  } else if (type->is_enum()) {
    return "org.apache.thrift.protocol.TType.I32";
  } else if (type->is_struct() || type->is_xception()) {
    return "org.apache.thrift.protocol.TType.STRUCT";
  } else if (type->is_map()) {
    return "org.apache.thrift.protocol.TType.MAP";
  } else if (type->is_set()) {
    return "org.apache.thrift.protocol.TType.SET";
  } else if (type->is_list()) {
    return "org.apache.thrift.protocol.TType.LIST";
  }

  throw "INVALID TYPE IN type_to_enum: " + type->get_name();
}

/**
 * Takes a name and produes a valid kotlin source file name from it
 *
 * @param fromName The name which shall become a valid kotlin source file name
 * @return The produced identifier
 */
std::string t_kotlin_generator::make_valid_kotlin_filename(std::string const &fromName) {
  // if any further rules apply to source file names in kotlin, modify as necessary
  return make_valid_kotlin_identifier(fromName);
}

/**
 * Takes a name and produes a valid kotlin identifier from it
 *
 * @param fromName The name which shall become a valid kotlin identifier
 * @return The produced identifier
 */
std::string t_kotlin_generator::make_valid_kotlin_identifier(std::string const &fromName) {
  std::string str = fromName;
  if (str.empty()) {
    return str;
  }

  // tests rely on this
  assert(('A' < 'Z') && ('a' < 'z') && ('0' < '9'));

  // if the first letter is a number, we add an additional underscore in front of it
  char c = str.at(0);
  if (('0' <= c) && (c <= '9')) {
    str = "_" + str;
  }

  // following chars: letter, number or underscore
  for (size_t i = 0; i < str.size(); ++i) {
    c = str.at(i);
    if ((('A' > c) || (c > 'Z')) && (('a' > c) || (c > 'z')) && (('0' > c) || (c > '9'))
        && ('_' != c)) {
      str.replace(i, 1, "_");
    }
  }

  return str;
}

std::string t_kotlin_generator::as_camel_case(std::string name, bool ucfirst) {
  std::string new_name;
  size_t i = 0;
  for (i = 0; i < name.size(); i++) {
    if (name[i] != '_')
      break;
  }
  if (ucfirst) {
    new_name += toupper(name[i++]);
  } else {
    new_name += tolower(name[i++]);
  }
  for (; i < name.size(); i++) {
    if (name[i] == '_') {
      if (i < name.size() - 1) {
        i++;
        new_name += toupper(name[i]);
      }
    } else {
      new_name += name[i];
    }
  }
  return new_name;
}

std::string t_kotlin_generator::get_rpc_method_name(std::string name) {
  return name;
}

/**
 * Applies the correct style to a string based on the value of nocamel_style_
 * and/or fullcamel_style_
 */
std::string t_kotlin_generator::get_cap_name(std::string name) {
  name[0] = toupper(name[0]);
  return name;
}

string t_kotlin_generator::constant_name(const string &name) {
  string constant_name;

  bool is_first = true;
  bool was_previous_char_upper = false;
  for (char character : name) {
    bool is_upper = isupper(character);

    if (is_upper && !is_first && !was_previous_char_upper) {
      constant_name += '_';
    }
    constant_name += toupper(character);

    is_first = false;
    was_previous_char_upper = is_upper;
  }

  return constant_name;
}

void t_kotlin_generator::generate_deep_copy_container(ostream &out,
                                                      const std::string &source_name_p1,
                                                      const std::string &source_name_p2,
                                                      const std::string &result_name,
                                                      t_type *type) {

  t_container *container = (t_container *) type;
  std::string source_name;
  if (source_name_p2 == "")
    source_name = source_name_p1;
  else
    source_name = source_name_p1 + "." + source_name_p2;

  bool copy_construct_container;
  if (container->is_map()) {
    t_map *tmap = (t_map *) container;
    copy_construct_container = tmap->get_key_type()->is_base_type()
                               && tmap->get_val_type()->is_base_type();
  } else {
    t_type *elem_type = container->is_list() ? ((t_list *) container)->get_elem_type()
                                             : ((t_set *) container)->get_elem_type();
    copy_construct_container = elem_type->is_base_type();
  }

  if (copy_construct_container) {
    // deep copy of base types can be done much more efficiently than iterating over all the
    // elements manually
    indent(out) << "val " << result_name << ":" << type_name(type, true, false) << " = "
                << type_name(container, false, true) << "(" << source_name << ")" << endl;
    return;
  }

  std::string constructor_args;
  if (is_enum_set(container) || is_enum_map(container)) {
    constructor_args = inner_enum_type_name(container);
  } else if (!((container->is_map() || container->is_set()))) {
    // unsorted containers accept a capacity value
    constructor_args = source_name + "!!.size";
  }

  if (is_enum_set(container)) {
    indent(out) << type_name(type, true, false) << " " << result_name << " = "
                << type_name(container, false, true, true) << ".noneOf(" << constructor_args << ");" << endl;
  } else {
    indent(out) << "val " << result_name << ": " << type_name(type, true, false) << " = "
                << type_name(container, false, true) << "(" << constructor_args << ")" << endl;
  }

  std::string iterator_element_name = source_name_p1 + "_element";
  std::string result_element_name = result_name + "_copy";

  if (container->is_map()) {
    t_type *key_type = ((t_map *) container)->get_key_type();
    t_type *val_type = ((t_map *) container)->get_val_type();

    indent(out) << "for (" << iterator_element_name << ": MutableMap.MutableEntry<" << type_name(key_type, true, false) << ", "
                << type_name(val_type, true, false) << "> " << " in " << source_name << "?.entries!!) {" << endl;
    indent_up();

    out << endl;

    indent(out) << "val " << iterator_element_name << "_key: " << type_name(key_type, true, false)
                << " = " << iterator_element_name << ".key" << endl;
    indent(out) << "val " << iterator_element_name << "_value: " << type_name(val_type, true, false)
                << " = " << iterator_element_name << ".value" << endl;

    out << endl;

    if (key_type->is_container()) {
      generate_deep_copy_container(out,
                                   iterator_element_name + "_key",
                                   "",
                                   result_element_name + "_key",
                                   key_type);
    } else {
      indent(out) << "val " << result_element_name << "_key: " << type_name(key_type, true, false) << " = ";
      generate_deep_copy_non_container(out,
                                       iterator_element_name + "_key",
                                       result_element_name + "_key",
                                       key_type);
      out << endl;
    }

    out << endl;

    if (val_type->is_container()) {
      generate_deep_copy_container(out,
                                   iterator_element_name + "_value",
                                   "",
                                   result_element_name + "_value",
                                   val_type);
    } else {
      indent(out) << "val " << result_element_name << "_value: " << type_name(val_type, true, false) << " = ";
      generate_deep_copy_non_container(out,
                                       iterator_element_name + "_value",
                                       result_element_name + "_value",
                                       val_type);
      out << endl;
    }

    out << endl;

    indent(out) << result_name << ".put(" << result_element_name << "_key!!, " << result_element_name
                << "_value)" << endl;

    indent_down();
    indent(out) << "}" << endl;

  } else {
    t_type *elem_type;

    if (container->is_set()) {
      elem_type = ((t_set *) container)->get_elem_type();
    } else {
      elem_type = ((t_list *) container)->get_elem_type();
    }

    indent(out) << "for (" << iterator_element_name << ": " << type_name(elem_type, true, false)
                << " in " << source_name << "!!) {" << endl;

    indent_up();

    if (elem_type->is_container()) {
      // recursive deep copy
      generate_deep_copy_container(out, iterator_element_name, "", result_element_name, elem_type);
      indent(out) << result_name << ".add(" << result_element_name << ")" << endl;
    } else {
      // iterative copy
      if (elem_type->is_binary()) {
        indent(out) << "java.nio.ByteBuffer temp_binary_element = ";
        generate_deep_copy_non_container(out,
                                         iterator_element_name,
                                         "temp_binary_element",
                                         elem_type);
        out << endl;
        indent(out) << result_name << ".add(temp_binary_element);" << endl;
      } else {
        indent(out) << result_name << ".add(";
        generate_deep_copy_non_container(out, iterator_element_name, result_name, elem_type);
        out << ")" << endl;
      }
    }

    indent_down();

    indent(out) << "}" << endl;
  }
}

void t_kotlin_generator::generate_deep_copy_non_container(ostream &out,
                                                          const string &source_name,
                                                          const string &dest_name,
                                                          t_type *type) {
  (void) dest_name;
  type = get_true_type(type);
  if (type->is_base_type() || type->is_enum() || type->is_typedef()) {
    if (type->is_binary()) {
      out << "org.apache.thrift.TBaseHelper.copyBinary(" << source_name << "!!)";
    } else {
      // everything else can be copied directly
      out << source_name;
    }
  } else {
    out << "" << type_name(type, true, true) << "(" << source_name << "!!)";
  }
}

std::string t_kotlin_generator::generate_isset_check(t_field *field) {
  return generate_isset_check(field->get_name());
}

std::string t_kotlin_generator::isset_field_id(t_field *field) {
  return "__" + upcase_string(field->get_name() + "_isset_id");
}

std::string t_kotlin_generator::generate_isset_check(std::string field_name) {
  return "is" + get_cap_name("set") + get_cap_name(field_name) + "()";
}

void t_kotlin_generator::generate_isset_set(ostream &out, t_field *field, string prefix) {
  if (!type_can_be_null(field->get_type())) {
    indent(out) << prefix << "set" << get_cap_name(field->get_name()) << get_cap_name("isSet")
                << "(true);" << endl;
  }
}

void t_kotlin_generator::generate_struct_desc(ostream &out, t_struct *tstruct) {
  indent(out) << "private val STRUCT_DESC: org.apache.thrift.protocol.TStruct = "
                 "org.apache.thrift.protocol.TStruct(\"" << tstruct->get_name() << "\");" << endl;
}

void t_kotlin_generator::generate_field_descs(ostream &out, t_struct *tstruct) {
  const vector<t_field *> &members = tstruct->get_members();
  vector<t_field *>::const_iterator m_iter;

  for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
    indent(out) << "private val "
                << constant_name((*m_iter)->get_name())
                << "_FIELD_DESC: org.apache.thrift.protocol.TField "
                << "= org.apache.thrift.protocol.TField(\"" << (*m_iter)->get_name()
                << "\", " << type_to_enum((*m_iter)->get_type()) << ", "
                << (*m_iter)->get_key() << ")" << endl;
  }
}

void t_kotlin_generator::generate_scheme_map(ostream &out, t_struct *tstruct) {
  indent(out) << "private val STANDARD_SCHEME_FACTORY: org.apache.thrift.scheme.SchemeFactory"
              << "<" << tstruct->get_name() << "StandardScheme> = "
              << tstruct->get_name() << "StandardSchemeFactory()" << endl;
  indent(out) << "private val TUPLE_SCHEME_FACTORY: org.apache.thrift.scheme.SchemeFactory"
              << "<" << tstruct->get_name() << "TupleScheme> = "
              << tstruct->get_name() << "TupleSchemeFactory()" << endl;
}

void t_kotlin_generator::generate_field_name_constants(ostream &out, t_struct *tstruct) {
  indent(out) << "/** The set of fields this struct contains, along with convenience methods for "
                 "finding and manipulating them. */" << endl;
  indent(out) << "enum class Fields(" << endl;
  indent_up();
  out << indent() << "override val thriftFieldId: Short," << endl
      << indent() << "override val fieldName: String" << endl;
  indent_down();
  indent(out) << ") : org.apache.thrift.TFieldIdEnum {" << endl;

  indent_up();
  bool first = true;
  const vector<t_field *> &members = tstruct->get_members();
  vector<t_field *>::const_iterator m_iter;
  for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
    if (!first) {
      out << "," << endl;
    }
    first = false;
    generate_java_doc(out, *m_iter);
    indent(out) << constant_name((*m_iter)->get_name()) << "(" << (*m_iter)->get_key()
                << ", \"" << (*m_iter)->get_name() << "\")";
  }

  out << ";" << endl << endl;

  // Start of companion object
  indent(out) << "companion object {" << endl;
  indent_up();
  indent(out) << "var byName: Map<String, Fields> = mapOf(" << endl;
  indent_up();
  first = true;
  for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
    if (!first) {
      out << "," << endl;
    }
    first = false;
    generate_java_doc(out, *m_iter);
    indent(out) << "\"" << (*m_iter)->get_name() << "\" to " << constant_name((*m_iter)->get_name());
  }
  indent_down();
  out << endl << indent() << ")" << endl;

  indent(out) << "/**" << endl;
  indent(out) << " * Find the Fields constant that matches fieldId, or null if its not found."
              << endl;
  indent(out) << " */" << endl;
  indent(out) << "fun findByThriftId(fieldId: Short): Fields? {" << endl;
  indent_up();
  indent(out) << "return when(fieldId) {" << endl;
  indent_up();

  for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
    indent(out) << "(" << (*m_iter)->get_key() << ").toShort() -> // "
                << constant_name((*m_iter)->get_name()) << endl;
    indent(out) << "  " << constant_name((*m_iter)->get_name()) << endl;
  }

  indent(out) << "else ->" << endl;
  indent(out) << "  null" << endl;

  indent_down();
  indent(out) << "}" << endl;

  indent_down();
  indent(out) << "}" << endl << endl;

  indent(out) << "/**" << endl;
  indent(out) << " * Find the Fields constant that matches fieldId, throwing an exception" << endl;
  indent(out) << " * if it is not found." << endl;
  indent(out) << " */" << endl;
  indent(out) << "fun findByThriftIdOrThrow(fieldId: Short): Fields {" << endl;
  indent(out) << "  return findByThriftId(fieldId)" << endl;
  indent(out) << "    ?: throw java.lang.IllegalArgumentException(\"Field $fieldId doesn't exist!\");" << endl;
  indent(out) << "}" << endl << endl;

  indent(out) << "/**" << endl;
  indent(out) << " * Find the Fields constant that matches name, or null if its not found."
              << endl;
  indent(out) << " */" << endl;
  indent(out) << "fun findByName(name: String): Fields? {" << endl;
  indent(out) << "  return byName.get(name)" << endl;
  indent(out) << "}" << endl;

  // End of companion object
  scope_down(out);

  // End of enum
  scope_down(out);
}

t_kotlin_generator::isset_type t_kotlin_generator::needs_isset(t_struct *tstruct,
                                                               std::string *outPrimitiveType) {
  const vector<t_field *> &members = tstruct->get_members();
  vector<t_field *>::const_iterator m_iter;

  int count = 0;
  for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
    if (!type_can_be_null(get_true_type((*m_iter)->get_type()))) {
      count++;
    }
  }
  if (count == 0) {
    return ISSET_NONE;
  } else if (count <= 64) {
    if (outPrimitiveType != nullptr) {
      if (count <= 8)
        *outPrimitiveType = "Byte";
      else if (count <= 16)
        *outPrimitiveType = "Short";
      else if (count <= 32)
        *outPrimitiveType = "Int";
      else if (count <= 64)
        *outPrimitiveType = "Long";
    }
    return ISSET_PRIMITIVE;
  } else {
    return ISSET_BITSET;
  }
}

void t_kotlin_generator::generate_kotlin_struct_clear(std::ostream &out, t_struct *tstruct) {
  indent(out) << "override fun clear() {" << endl;

  const vector<t_field *> &members = tstruct->get_members();
  vector<t_field *>::const_iterator m_iter;

  indent_up();
  for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
    t_field *field = *m_iter;
    t_type *t = get_true_type(field->get_type());
    std::string field_name = field->get_name();

    if (field->get_value() != nullptr) {
      print_const_value(out, "this." + field_name, t, field->get_value(), true, true);
      continue;
    }

    if (type_can_be_null(t)) {

      if (t->is_container() || t->is_struct()) {
        indent(out) << "this." << field_name << "?.clear()" << endl;
      } else {
        indent(out) << "this." << field_name << " = null" << endl;
      }
      continue;
    }

    // must be a base type
    // means it also needs to be explicitly unset
    indent(out) << "set" << get_cap_name(field_name) << get_cap_name("isSet") << "(false)"
                << endl;
    t_base_type *base_type = (t_base_type *) t;

    switch (base_type->get_base()) {
      case t_base_type::TYPE_I8:
      case t_base_type::TYPE_I16:
      case t_base_type::TYPE_I32:
      case t_base_type::TYPE_I64:
        indent(out) << "this." << field_name << " = 0" << endl;
        break;
      case t_base_type::TYPE_DOUBLE:
        indent(out) << "this." << field_name << " = 0.0" << endl;
        break;
      case t_base_type::TYPE_BOOL:
        indent(out) << "this." << field_name << " = false" << endl;
        break;
      default:
        throw "unsupported type: " + base_type->get_name() + " for field " + field_name;
    }
  }
  indent_down();

  indent(out) << "}" << endl << endl;
}

// generates kotlin method to serialize (in the kotlin sense) the object
void t_kotlin_generator::generate_kotlin_struct_write_object(ostream &out, t_struct *tstruct) {
  (void) tstruct;
  indent(out)
          << "suspend fun writeObject(out:java.nio.channels.AsynchronousByteChannel) {"
          << endl;
  indent(out) << "  try {" << endl;
  indent(out) << "    write(org.apache.thrift.protocol.TCompactProtocol("
                 "org.apache.thrift.transport.TIOStreamTransport(out)))" << endl;
  indent(out) << "  } catch (te:org.apache.thrift.TException) {" << endl;
  indent(out) << "    throw java.io.IOException(te"
              << ");" << endl;
  indent(out) << "  }" << endl;
  indent(out) << "}" << endl << endl;
}

// generates kotlin method to serialize the object
void t_kotlin_generator::generate_kotlin_struct_read_object(ostream &out, t_struct *tstruct) {
  indent(out) << "suspend fun readObject(inp:java.nio.channels.AsynchronousByteChannel) {" << endl;
  indent(out) << "  try {" << endl;
  if (!tstruct->is_union()) {
    switch (needs_isset(tstruct)) {
      case ISSET_NONE:
        break;
      case ISSET_PRIMITIVE:
        indent(out) << "    __isset_bitfield = 0" << endl;
        break;
      case ISSET_BITSET:
        indent(out) << "    __isset_bit_vector = java.util.BitSet(1)" << endl;
        break;
    }
  }
  indent(out) << "    read(org.apache.thrift.protocol.TCompactProtocol("
                 "org.apache.thrift.transport.TIOStreamTransport(inp)))" << endl;
  indent(out) << "  } catch (te:org.apache.thrift.TException) {" << endl;
  indent(out) << "    throw java.io.IOException(te"
              << ");" << endl;
  indent(out) << "  }" << endl;
  indent(out) << "}" << endl << endl;
}

void t_kotlin_generator::generate_standard_reader(ostream &out, t_struct *tstruct) {
  out << indent() << "override suspend fun read(iprot:org.apache.thrift.protocol.TProtocol, "
      << "struct:" << tstruct->get_name() << ") {" << endl;
  indent_up();

  const vector<t_field *> &fields = tstruct->get_members();
  vector<t_field *>::const_iterator f_iter;

  // Declare stack tmp variables and read struct header
  out << indent() << "lateinit var schemeField: org.apache.thrift.protocol.TField" << endl << indent()
      << "iprot.readStructBegin();" << endl;

  // Loop over reading in fields
  indent(out) << "while (true)" << endl;
  scope_up(out);

  // Read beginning field marker
  indent(out) << "schemeField = iprot.readFieldBegin()" << endl;

  // Check for field STOP marker and break
  indent(out) << "if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { " << endl;
  indent_up();
  indent(out) << "break" << endl;
  indent_down();
  indent(out) << "}" << endl;

  // Switch statement on the field we are reading
  // Kotlin cant switch on a short so first convert to int
  indent(out) << "when (schemeField.id.toInt()) {" << endl;

  indent_up();

  // Generate deserialization code for known cases
  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    indent(out) << (*f_iter)->get_key() << " -> // "
                << constant_name((*f_iter)->get_name()) << endl;
    indent_up();
    indent(out) << "if (schemeField.type == " << type_to_enum((*f_iter)->get_type()) << ") {"
                << endl;
    indent_up();

    generate_deserialize_field(out, *f_iter, "struct.", true);
    indent(out) << "struct.set" << capitalize((*f_iter)->get_name()) << get_cap_name("isSet") << "(true);" << endl;
    indent_down();
    out << indent() << "} else { " << endl << indent()
        << "  org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);" << endl
        << indent() << "}" << endl;
    indent_down();
  }

  indent(out) << "else ->" << endl;
  indent(out) << "  org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);"
              << endl;

  indent_down();
  indent(out) << "}" << endl;

  // Read field end marker
  indent(out) << "iprot.readFieldEnd();" << endl;

  indent_down();
  indent(out) << "}" << endl;

  out << indent() << "iprot.readStructEnd();" << endl;

  // in non-beans style, check for required fields of primitive type
  // (which can be checked here but not in the general validate method)
  out << endl << indent() << "// check for required fields of primitive type, which can't be "
                             "checked in the validate method" << endl;
  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    if ((*f_iter)->get_req() == t_field::T_REQUIRED && !type_can_be_null((*f_iter)->get_type())) {
      out << indent() << "if (!struct." << generate_isset_check(*f_iter) << ") {" << endl
          << indent()
          << "  throw org.apache.thrift.protocol.TProtocolException(\"Required field '"
          << (*f_iter)->get_name()
          << "' was not found in serialized data! Struct: \" + toString());" << endl << indent()
          << "}" << endl;
    }
  }

  // performs various checks (e.g. check that all required fields are set)
  indent(out) << "struct.validate();" << endl;

  indent_down();
  out << indent() << "}" << endl;
}

void t_kotlin_generator::generate_standard_writer(ostream &out, t_struct *tstruct, bool is_result) {
  indent_up();
  out << indent() << "override suspend fun write(oprot:org.apache.thrift.protocol.TProtocol, "
      << "struct:" << tstruct->get_name() << ") {" << endl;
  indent_up();
  const vector<t_field *> &fields = tstruct->get_sorted_members();
  vector<t_field *>::const_iterator f_iter;

  // performs various checks (e.g. check that all required fields are set)
  indent(out) << "struct.validate()" << endl << endl;

  indent(out) << "oprot.writeStructBegin(STRUCT_DESC)" << endl;

  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    bool null_allowed = type_can_be_null((*f_iter)->get_type());
    if (null_allowed) {
      out << indent() << "if (struct." << (*f_iter)->get_name() << " != null) {" << endl;
      indent_up();
    }
    bool optional = ((*f_iter)->get_req() == t_field::T_OPTIONAL) || (is_result && !null_allowed);
    if (optional) {
      indent(out) << "if ("
                  << "struct." << generate_isset_check((*f_iter)) << ") {" << endl;
      indent_up();
    }

    indent(out) << "oprot.writeFieldBegin(" << constant_name((*f_iter)->get_name())
                << "_FIELD_DESC)" << endl;

    // Write field contents
    generate_serialize_field(out, *f_iter, "struct.", true);

    // Write field closer
    indent(out) << "oprot.writeFieldEnd()" << endl;

    if (optional) {
      indent_down();
      indent(out) << "}" << endl;
    }
    if (null_allowed) {
      indent_down();
      indent(out) << "}" << endl;
    }
  }
  // Write the struct map
  out << indent() << "oprot.writeFieldStop()" << endl << indent() << "oprot.writeStructEnd()"
      << endl;

  indent_down();
  out << indent() << "}" << endl << endl;
  indent_down();
}

void t_kotlin_generator::generate_kotlin_struct_standard_scheme(ostream &out,
                                                                t_struct *tstruct,
                                                                bool is_result) {
  indent(out) << "private class " << tstruct->get_name()
              << "StandardSchemeFactory : org.apache.thrift.scheme.SchemeFactory<"
              << tstruct->get_name() << "StandardScheme> {" << endl;
  indent_up();
  indent(out) << "override fun getScheme():" << tstruct->get_name() << "StandardScheme {" << endl;
  indent_up();
  indent(out) << "return " << tstruct->get_name() << "StandardScheme()" << endl;
  indent_down();
  indent(out) << "}" << endl;
  indent_down();
  indent(out) << "}" << endl << endl;

  out << indent() << "private class " << tstruct->get_name()
      << "StandardScheme : org.apache.thrift.scheme.StandardScheme<" << tstruct->get_name() << ">() {" << endl
      << endl;
  indent_up();
  generate_standard_reader(out, tstruct);
  indent_down();
  out << endl;
  generate_standard_writer(out, tstruct, is_result);

  out << indent() << "}" << endl << endl;
}

void t_kotlin_generator::generate_kotlin_struct_tuple_reader(ostream &out, t_struct *tstruct) {
  indent(out) << "override suspend fun read(iprot: org.apache.thrift.protocol.TProtocol"
              << ", struct:" << tstruct->get_name() << ") {" << endl;
  indent_up();
  indent(out) << "val prot:org.apache.thrift.protocol.TTupleProtocol = "
              << "iprot as org.apache.thrift.protocol.TTupleProtocol" << endl;
  int optional_count = 0;
  const vector<t_field *> &fields = tstruct->get_members();
  vector<t_field *>::const_iterator f_iter;
  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    if ((*f_iter)->get_req() == t_field::T_OPTIONAL
        || (*f_iter)->get_req() == t_field::T_OPT_IN_REQ_OUT) {
      optional_count++;
    }
    if ((*f_iter)->get_req() == t_field::T_REQUIRED) {
      generate_deserialize_field(out, (*f_iter), "struct.", false);
      indent(out) << "struct.set" << get_cap_name((*f_iter)->get_name()) << get_cap_name("isSet")
                  << "(true);" << endl;
    }
  }
  if (optional_count > 0) {
    indent(out) << "val incoming:java.util.BitSet = prot.readBitSet(" << optional_count << ")" << endl;
    int i = 0;
    for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
      if ((*f_iter)->get_req() == t_field::T_OPTIONAL
          || (*f_iter)->get_req() == t_field::T_OPT_IN_REQ_OUT) {
        indent(out) << "if (incoming.get(" << i << ")) {" << endl;
        indent_up();
        generate_deserialize_field(out, (*f_iter), "struct.", false);
        indent(out) << "struct.set" << get_cap_name((*f_iter)->get_name()) << get_cap_name("isSet")
                    << "(true)" << endl;
        indent_down();
        indent(out) << "}" << endl;
        i++;
      }
    }
  }
  indent_down();
  indent(out) << "}" << endl;
}

void t_kotlin_generator::generate_kotlin_struct_tuple_writer(ostream &out, t_struct *tstruct) {
  indent(out) << "override suspend fun write(oprot:org.apache.thrift.protocol.TProtocol"
              << ", struct:" << tstruct->get_name() << ") {" << endl;
  indent_up();
  indent(out) << "val prot:org.apache.thrift.protocol.TTupleProtocol = "
              << "oprot as org.apache.thrift.protocol.TTupleProtocol" << endl;

  const vector<t_field *> &fields = tstruct->get_members();
  vector<t_field *>::const_iterator f_iter;
  bool has_optional = false;
  int optional_count = 0;
  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    if ((*f_iter)->get_req() == t_field::T_OPTIONAL
        || (*f_iter)->get_req() == t_field::T_OPT_IN_REQ_OUT) {
      optional_count++;
      has_optional = true;
    }
    if ((*f_iter)->get_req() == t_field::T_REQUIRED) {
      generate_serialize_field(out, (*f_iter), "struct.", false);
    }
  }
  if (has_optional) {
    indent(out) << "val optionals:java.util.BitSet = java.util.BitSet()" << endl;
    int i = 0;
    for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
      if ((*f_iter)->get_req() == t_field::T_OPTIONAL
          || (*f_iter)->get_req() == t_field::T_OPT_IN_REQ_OUT) {
        indent(out) << "if (struct." << generate_isset_check((*f_iter)) << ") {" << endl;
        indent_up();
        indent(out) << "optionals.set(" << i << ")" << endl;
        indent_down();
        indent(out) << "}" << endl;
        i++;
      }
    }

    indent(out) << "prot.writeBitSet(optionals, " << optional_count << ")" << endl;
    int j = 0;
    for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
      if ((*f_iter)->get_req() == t_field::T_OPTIONAL
          || (*f_iter)->get_req() == t_field::T_OPT_IN_REQ_OUT) {
        indent(out) << "if (struct." << generate_isset_check(*f_iter) << ") {" << endl;
        indent_up();
        generate_serialize_field(out, (*f_iter), "struct.", false);
        indent_down();
        indent(out) << "}" << endl;
        j++;
      }
    }
  }
  indent_down();
  indent(out) << "}" << endl;
}

void t_kotlin_generator::generate_kotlin_struct_tuple_scheme(ostream &out, t_struct *tstruct) {
  indent(out) << "private class " << tstruct->get_name()
              << "TupleSchemeFactory : org.apache.thrift.scheme.SchemeFactory<" << tstruct->get_name()
              << "TupleScheme> {" << endl;
  indent_up();
  indent(out) << "override fun getScheme():" << tstruct->get_name() << "TupleScheme {" << endl;
  indent_up();
  indent(out) << "return " << tstruct->get_name() << "TupleScheme()" << endl;
  indent_down();
  indent(out) << "}" << endl;
  indent_down();
  indent(out) << "}" << endl << endl;
  out << indent() << "private class " << tstruct->get_name()
      << "TupleScheme : org.apache.thrift.scheme.TupleScheme<" << tstruct->get_name() << ">() {" << endl << endl;
  indent_up();
  generate_kotlin_struct_tuple_writer(out, tstruct);
  out << endl;
  generate_kotlin_struct_tuple_reader(out, tstruct);
  indent_down();
  out << indent() << "}" << endl << endl;
}

void t_kotlin_generator::generate_kotlin_scheme_lookup(ostream &out) {
  indent_up();
  indent(out) << "private fun <S : org.apache.thrift.TBase<S, *>> scheme("
              << "proto:org.apache.thrift.protocol.TProtocol): org.apache.thrift.scheme.IScheme<S> {" << endl;
  indent_up();
  indent(out) << "if(proto.scheme.isAssignableFrom(org.apache.thrift.scheme.StandardScheme::class.java)) {" << endl;
  indent_up();
  indent(out) << "return STANDARD_SCHEME_FACTORY.getScheme() as org.apache.thrift.scheme.IScheme<S>" << endl;
  scope_down(out);
  indent(out) << "return TUPLE_SCHEME_FACTORY.getScheme() as org.apache.thrift.scheme.IScheme<S>" << endl;
  indent_down();
  indent(out) << "}" << endl;
  indent_down();
}

void t_kotlin_generator::generate_java_generated_annotation(ostream &out) {
  time_t seconds = time(nullptr);
  struct tm *now = localtime(&seconds);
  indent(out) << "@javax.annotation.processing.Generated(value = [\"" << autogen_summary() << "\"]";
  indent(out) << ", date = \"" << (now->tm_year + 1900) << "-" << setfill('0') << setw(2)
              << (now->tm_mon + 1) << "-" << setfill('0') << setw(2) << now->tm_mday
              << "\")" << endl;
}

THRIFT_REGISTER_GENERATOR(
        kotlin,
        "kotlin",
        "    beans:           Members will be private, and setter methods will return void.\n"
        "    private-members: Members will be private, but setter methods will return 'this' like "
        "usual.\n"
        "    nocamel:         Do not use CamelCase field accessors with beans.\n"
        "    fullcamel:       Convert underscored_accessor_or_service_names to camelCase.\n"
        "    android:         Generated structures are Parcelable.\n"
        "    android_legacy:  Do not use kotlin.io.IOException(throwable) (available for Android 2.3 and "
        "above).\n"
        "    option_type:     Wrap optional fields in an Option type.\n"
        "    rethrow_unhandled_exceptions:\n"
        "                     Enable rethrow of unhandled exceptions and let them propagate further."
        " (Default behavior is to catch and log it.)\n"
        "    kotlin5:           Generate kotlin 1.5 compliant code (includes android_legacy flag).\n"
        "    reuse-objects:   Data objects will not be allocated, but existing instances will be used "
        "(read and write).\n"
        "    sorted_containers:\n"
        "                     Use TreeSet/TreeMap instead of HashSet/HashMap as a implementation of "
        "set/map.\n"
        "    generated_annotations=[undated|suppress]:\n"
        "                     undated: suppress the date at @Generated annotations\n"
        "                     suppress: suppress @Generated annotations entirely\n"
        "    unsafe_binaries: Do not copy ByteBuffers in constructors, getters, and setters.\n")
