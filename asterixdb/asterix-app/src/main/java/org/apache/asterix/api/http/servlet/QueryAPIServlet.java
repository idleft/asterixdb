/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.api.http.servlet;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.lang.common.base.Statement;

public class QueryAPIServlet extends RESTAPIServlet {
    private static final long serialVersionUID = 1L;

    private static final List<Byte> allowedStatements = Collections.unmodifiableList(Arrays.asList(
            Statement.Kind.DATAVERSE_DECL, Statement.Kind.FUNCTION_DECL, Statement.Kind.QUERY, Statement.Kind.SET,
            Statement.Kind.WRITE, Statement.Kind.RUN));

    public QueryAPIServlet(ILangCompilationProvider compilationProvider) {
        super(compilationProvider);
    }

    @Override
    protected String getQueryParameter(HttpServletRequest request) {
        return request.getParameter("query");
    }

    @Override
    protected List<Byte> getAllowedStatements() {
        return allowedStatements;
    }

    @Override
    protected String getErrorMessage() {
        return "Invalid statement: Non-query statement %s to the query API.";
    }
}
