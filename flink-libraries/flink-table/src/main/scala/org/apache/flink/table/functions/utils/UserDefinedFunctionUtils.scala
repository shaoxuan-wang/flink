/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.table.functions.utils

import java.lang.{Long => JLong, Integer => JInt}
import java.lang.reflect.{Method, Modifier}
import java.sql.{Date, Time, Timestamp}

import org.apache.commons.codec.binary.Base64
import com.google.common.primitives.Primitives
import org.apache.calcite.sql.SqlFunction
import org.apache.flink.api.common.functions.InvalidTypesException
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.api.{TableEnvironment, ValidationException}
import org.apache.flink.table.functions.{ScalarFunction, TableFunction, UserDefinedFunction, AggregateFunction}
import org.apache.flink.table.plan.schema.FlinkTableFunctionImpl
import org.apache.flink.util.InstantiationUtil

object UserDefinedFunctionUtils {

  /**
    * Checks if a user-defined function can be easily instantiated.
    */
  def checkForInstantiation(clazz: Class[_]): Unit = {
    if (!InstantiationUtil.isPublic(clazz)) {
      throw ValidationException("Function class is not public.")
    }
    else if (!InstantiationUtil.isProperClass(clazz)) {
      throw ValidationException("Function class is no proper class, it is either abstract," +
        " an interface, or a primitive type.")
    }
    else if (InstantiationUtil.isNonStaticInnerClass(clazz)) {
      throw ValidationException("The class is an inner class, but not statically accessible.")
    }
  }

  /**
    * Check whether this is a Scala object. It is forbidden to use [[TableFunction]] implemented
    * by a Scala object, since concurrent risks.
    */
  def checkNotSingleton(clazz: Class[_]): Unit = {
    // TODO it is not a good way to check singleton. Maybe improve it further.
    if (clazz.getFields.map(_.getName) contains "MODULE$") {
      throw new ValidationException(
        s"TableFunction implemented by class ${clazz.getCanonicalName} " +
          s"is a Scala object, it is forbidden since concurrent risks.")
    }
  }

  // ----------------------------------------------------------------------------------------------
  // Utilities for eval methods
  // ----------------------------------------------------------------------------------------------

  /**
    * Returns signatures matching the given signature of [[TypeInformation]].
    * Elements of the signature can be null (act as a wildcard).
    */
  def getEvalMethodSignature(
    function: UserDefinedFunction,
    signature: Seq[TypeInformation[_]])
    : Option[Array[Class[_]]] = {
    getUserDefinedMethod(function, "eval", typeInfoToClass(signature)).map(_.getParameterTypes)
  }

  def getAccumulateMethodSignature(
      function: AggregateFunction[_, _],
      signature: Seq[TypeInformation[_]])
  : Option[Array[Class[_]]] = {
    val accType = TypeExtractor.createTypeInfo(
      function, classOf[AggregateFunction[_, _]], function.getClass, 1)
    val input = Array(accType) ++ signature
    getUserDefinedMethod(
      function,
      "accumulate",
      typeInfoToClassArray(input)).map(_.getParameterTypes)
  }

  def getParameterTypes(
      function: UserDefinedFunction,
      signature: Array[Class[_]]): Array[TypeInformation[_]] = {
    signature.map { c =>
      try {
        TypeExtractor.getForClass(c)
      } catch {
        case ite: InvalidTypesException =>
          throw new ValidationException(
            s"Parameter types of function '${function.getClass.getCanonicalName}' cannot be " +
              s"automatically determined. Please provide type information manually.")
      }
    }
  }

  /**
    * Returns user defined method matching the given name and signature.
    *
    * @param function        function instance
    * @param methodName      method name
    * @param methodSignature an array of raw Java classes. We compare the raw Java classes not the
    *                        TypeInformation. TypeInformation does not matter during runtime (e.g.
    *                        within a MapFunction)
    */
  def getUserDefinedMethod(
      function: UserDefinedFunction,
      methodName: String,
      methodSignature: Array[Class[_]])
    : Option[Method] = {

    val methods = checkAndExtractMethods(function, methodName)

    val filtered = methods
      // go over all the methods and filter out matching methods
      .filter {
        case cur if !cur.isVarArgs =>
          val signatures = cur.getParameterTypes
          // match parameters of signature to actual parameters
          methodSignature.length == signatures.length &&
            signatures.zipWithIndex.forall { case (clazz, i) =>
              parameterTypeEquals(methodSignature(i), clazz)
          }
        case cur if cur.isVarArgs =>
          val signatures = cur.getParameterTypes
          methodSignature.zipWithIndex.forall {
            // non-varargs
            case (clazz, i) if i < signatures.length - 1  =>
              parameterTypeEquals(clazz, signatures(i))
            // varargs
            case (clazz, i) if i >= signatures.length - 1 =>
              parameterTypeEquals(clazz, signatures.last.getComponentType)
          } || (methodSignature.isEmpty && signatures.length == 1) // empty varargs
    }

    // if there is a fixed method, compiler will call this method preferentially
    val fixedMethodsCount = filtered.count(!_.isVarArgs)
    val found = filtered.filter { cur =>
      fixedMethodsCount > 0 && !cur.isVarArgs ||
      fixedMethodsCount == 0 && cur.isVarArgs
    }

    // check if there is a Scala varargs annotation
    if (found.isEmpty &&
      methods.exists { method =>
        val signatures = method.getParameterTypes
        signatures.zipWithIndex.forall {
          case (clazz, i) if i < signatures.length - 1 =>
            parameterTypeEquals(methodSignature(i), clazz)
          case (clazz, i) if i == signatures.length - 1 =>
            clazz.getName.equals("scala.collection.Seq")
        }
      }) {
      throw new ValidationException(s"Scala-style variable arguments in '${methodName}' methods " +
                                      s"are not supported. Please add a " +
                                      s"@scala.annotation.varargs annotation.")
    } else if (found.length > 1) {
      throw new ValidationException("Found multiple '${methodName}' methods which match the " +
                                      "signature.")
    }
    found.headOption
  }

  /**
    * Check if a given method exists in the given function
    */
  def ifMethodExistInFunction(method: String, function: UserDefinedFunction): Boolean = {
    val methods = function
      .getClass
      .getMethods
      .filter {
        m => m.getName == method
      }
    !methods.isEmpty
  }

  /**
    * Extracts methods and throws a [[ValidationException]] if no implementation
    * can be found, or implementation does not match the requirements.
    */
  def checkAndExtractMethods(
      function: UserDefinedFunction,
      methodName: String): Array[Method] = {
    val methods = function
      .getClass
      .getMethods
      .filter { m =>
        val modifiers = m.getModifiers
        m.getName == methodName &&
          Modifier.isPublic(modifiers) &&
          !Modifier.isAbstract(modifiers) &&
          !(function.isInstanceOf[TableFunction[_]] && Modifier.isStatic(modifiers))
      }

    if (methods.isEmpty) {
      throw new ValidationException(
        s"Function class '${function.getClass.getCanonicalName}' does not implement at least " +
          s"one method named '${methodName}' which is public, not abstract and " +
          s"(in case of table functions) not static.")
    }

    methods
  }

  def getMethodSignatures(
      function: UserDefinedFunction,
      methodName: String): Array[Array[Class[_]]] = {
    checkAndExtractMethods(function, methodName).map(_.getParameterTypes)
  }

  // ----------------------------------------------------------------------------------------------
  // Utilities for SQL functions
  // ----------------------------------------------------------------------------------------------

  /**
    * Create [[SqlFunction]] for a [[ScalarFunction]]
    *
    * @param name function name
    * @param function scalar function
    * @param typeFactory type factory
    * @return the ScalarSqlFunction
    */
  def createScalarSqlFunction(
      name: String,
      function: ScalarFunction,
      typeFactory: FlinkTypeFactory)
    : SqlFunction = {
    new ScalarSqlFunction(name, function, typeFactory)
  }

  /**
    * Create [[SqlFunction]]s for a [[TableFunction]]'s every eval method
    *
    * @param name function name
    * @param tableFunction table function
    * @param resultType the type information of returned table
    * @param typeFactory type factory
    * @return the TableSqlFunction
    */
  def createTableSqlFunctions(
      name: String,
      tableFunction: TableFunction[_],
      resultType: TypeInformation[_],
      typeFactory: FlinkTypeFactory)
    : Seq[SqlFunction] = {
    val (fieldNames, fieldIndexes, _) = UserDefinedFunctionUtils.getFieldInfo(resultType)
    val evalMethods = checkAndExtractMethods(tableFunction, "eval")

    evalMethods.map { method =>
      val function = new FlinkTableFunctionImpl(resultType, fieldIndexes, fieldNames, method)
      TableSqlFunction(name, tableFunction, resultType, typeFactory, function)
    }
  }

  /**
    * Create [[SqlFunction]]s for a [[AggregateFunction]]'s every eval method
    *
    * @param name function name
    * @param aggFunction aggregate function
    * @param resultType the type information of returned table
    * @param typeFactory type factory
    * @return the TableSqlFunction
    */
  def createAggregateSqlFunctions(
      name: String,
      aggFunction: AggregateFunction[_, _],
      resultType: TypeInformation[_],
      typeFactory: FlinkTypeFactory)
  : Seq[SqlFunction] = {
    val (fieldNames, fieldIndexes, _) = UserDefinedFunctionUtils.getFieldInfo(resultType)
    val accumulateMethods = checkAndExtractMethods(aggFunction, "accumulate")

    accumulateMethods.map { method =>
//      val function = new FlinkTableFunctionImpl(resultType, fieldIndexes, fieldNames, method)
      AggSqlFunctionObj(name, aggFunction, resultType, resultType, typeFactory)
    }
  }

  // ----------------------------------------------------------------------------------------------
  // Utilities for scalar functions
  // ----------------------------------------------------------------------------------------------

  /**
    * Internal method of [[ScalarFunction#getResultType()]] that does some pre-checking and uses
    * [[TypeExtractor]] as default return type inference.
    */
  def getResultType(
      function: ScalarFunction,
      signature: Array[Class[_]])
    : TypeInformation[_] = {
    // find method for signature
    val evalMethod = checkAndExtractMethods(function, "eval")
      .find(m => signature.sameElements(m.getParameterTypes))
      .getOrElse(throw new ValidationException("Given signature is invalid."))

    val userDefinedTypeInfo = function.getResultType(signature)
    if (userDefinedTypeInfo != null) {
      userDefinedTypeInfo
    } else {
      try {
        TypeExtractor.getForClass(evalMethod.getReturnType)
      } catch {
        case ite: InvalidTypesException =>
          throw new ValidationException(
            s"Return type of scalar function '${function.getClass.getCanonicalName}' cannot be " +
              s"automatically determined. Please provide type information manually.")
      }
    }
  }

  /**
    * Returns the return type of the evaluation method matching the given signature.
    */
  def getResultTypeClass(
      function: ScalarFunction,
      signature: Array[Class[_]])
    : Class[_] = {
    // find method for signature
    val evalMethod = checkAndExtractMethods(function, "eval")
      .find(m => signature.sameElements(m.getParameterTypes))
      .getOrElse(throw new IllegalArgumentException("Given signature is invalid."))
    evalMethod.getReturnType
  }

  // ----------------------------------------------------------------------------------------------
  // Miscellaneous
  // ----------------------------------------------------------------------------------------------

  /**
    * Returns field names and field positions for a given [[TypeInformation]].
    *
    * Field names are automatically extracted for
    * [[org.apache.flink.api.common.typeutils.CompositeType]].
    *
    * @param inputType The TypeInformation to extract the field names and positions from.
    * @return A tuple of two arrays holding the field names and corresponding field positions.
    */
  def getFieldInfo(inputType: TypeInformation[_])
    : (Array[String], Array[Int], Array[TypeInformation[_]]) = {

    (TableEnvironment.getFieldNames(inputType),
    TableEnvironment.getFieldIndices(inputType),
    TableEnvironment.getFieldTypes(inputType))
  }

  /**
    * Prints one signature consisting of classes.
    */
  def signatureToString(signature: Array[Class[_]]): String =
  signature.map { clazz =>
    if (clazz == null) {
      "null"
    } else {
      clazz.getCanonicalName
    }
  }.mkString("(", ", ", ")")

  /**
    * Prints one signature consisting of TypeInformation.
    */
  def signatureToString(signature: Seq[TypeInformation[_]]): String = {
    signatureToString(typeInfoToClass(signature))
  }

  /**
    * Prints all eval methods signatures of a class.
    */
  def signaturesToString(function: UserDefinedFunction): String = {
    getMethodSignatures(function, "eval").map(signatureToString).mkString(", ")
  }

  /**
    * Extracts type classes of [[TypeInformation]] in a null-aware way.
    */
  def typeInfoToClass(typeInfos: Seq[TypeInformation[_]]): Array[Class[_]] =
  typeInfos.map { typeInfo =>
    if (typeInfo == null) {
      null
    } else {
      typeInfo.getTypeClass
    }
  }.toArray

  private def typeInfoToClassArray(typeInfos: Array[TypeInformation[_]]): Array[Class[_]] =
    typeInfos.map { typeInfo =>
      if (typeInfo == null) {
        null
      } else {
        typeInfo.getTypeClass
      }
    }

  /**
    * Compares parameter candidate classes with expected classes. If true, the parameters match.
    * Candidate can be null (acts as a wildcard).
    */
  private def parameterTypeEquals(candidate: Class[_], expected: Class[_]): Boolean =
  candidate == null ||
    candidate == expected ||
    expected == classOf[Object] ||
    expected.isPrimitive && Primitives.wrap(expected) == candidate ||
    candidate == classOf[Date] && (expected == classOf[Int] || expected == classOf[JInt])  ||
    candidate == classOf[Time] && (expected == classOf[Int] || expected == classOf[JInt]) ||
    candidate == classOf[Timestamp] && (expected == classOf[Long] || expected == classOf[JLong])

  @throws[Exception]
  def serialize(function: UserDefinedFunction): String = {
    val byteArray = InstantiationUtil.serializeObject(function)
    Base64.encodeBase64URLSafeString(byteArray)
  }

  @throws[Exception]
  def deserialize(data: String): UserDefinedFunction = {
    val byteData = Base64.decodeBase64(data)
    InstantiationUtil
      .deserializeObject[UserDefinedFunction](byteData, Thread.currentThread.getContextClassLoader)
  }
}
