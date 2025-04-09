/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shenyu.plugin.mcp.server;

import org.apache.shenyu.common.enums.PluginEnum;
import org.apache.shenyu.common.enums.PluginTypeEnum;
import org.apache.shenyu.common.utils.GsonUtils;
import org.springframework.ai.tool.annotation.Tool;

public class ShenyuPluginService {
    
    @Tool(description = "return all plugin enums")
    public String listAllPlugins() {
        PluginEnum[] pluginEnums = PluginEnum.values();
        return GsonUtils.getInstance().toJson(pluginEnums);
    }
    
    @Tool(description = "return all plugin type enums")
    public String listAllPluginTypeEnums() {
        PluginTypeEnum[] pluginTypeEnums = PluginTypeEnum.values();
        return GsonUtils.getInstance().toJson(pluginTypeEnums);
    }
    
}
