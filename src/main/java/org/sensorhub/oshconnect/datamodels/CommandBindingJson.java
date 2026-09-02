/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are subject to the Mozilla Public License, v. 2.0.
 If a copy of the MPL was not distributed with this file, You can obtain one
 at http://mozilla.org/MPL/2.0/.

 Software distributed under the License is distributed on an "AS IS" basis,
 WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 for the specific language governing rights and limitations under the License.

 Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 ******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.oshconnect.datamodels;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import net.opengis.swe.v20.BinaryBlock;
import net.opengis.swe.v20.BinaryEncoding;
import net.opengis.swe.v20.DataComponent;
import org.sensorhub.api.command.ICommandStreamInfo;
import org.sensorhub.api.common.BigId;
import org.sensorhub.api.common.IdEncoders;
import org.sensorhub.impl.service.consys.ResourceParseException;
import org.sensorhub.impl.service.consys.resource.PropertyFilter;
import org.sensorhub.impl.service.consys.resource.RequestContext;
import org.sensorhub.impl.service.consys.resource.ResourceBindingJson;
import org.sensorhub.impl.service.consys.resource.ResourceLink;
import org.sensorhub.impl.service.consys.task.CommandHandler.CommandHandlerContextData;
import org.sensorhub.utils.SWEDataUtils;
import org.vast.cdm.common.DataStreamWriter;
import org.vast.swe.BinaryDataWriter;
import org.vast.swe.IComponentFilter;
import org.vast.swe.SWEConstants;
import org.vast.swe.ScalarIndexer;
import org.vast.swe.fast.JsonDataParserGson;
import org.vast.swe.fast.JsonDataWriterGson;
import org.vast.util.ReaderException;

import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public class CommandBindingJson extends ResourceBindingJson<String, CommandData> {
    CommandHandlerContextData contextData;
    JsonDataParserGson paramsReader;
    Map<BigId, DataStreamWriter> paramsWriters;
    String userID;
    ScalarIndexer timeStampIndexer;

    public CommandBindingJson(RequestContext ctx, IdEncoders idEncoders, boolean forReading) throws IOException {
        super(ctx, idEncoders, forReading);
        this.contextData = (CommandHandlerContextData) ctx.getData();

        if (forReading) {
            this.paramsReader = getSweCommonParser(contextData.csInfo, reader);
            this.userID = "api";
            timeStampIndexer = SWEDataUtils.getTimeStampIndexer(contextData.csInfo.getRecordStructure());
        } else {
            this.paramsWriters = new HashMap<>();

            // init params writer only in case of single command stream
            // otherwise we'll do it later
            if (contextData.csInfo != null) {
                var resultWriter = getSweCommonWriter(contextData.csInfo, writer, ctx.getPropertyFilter());
                paramsWriters.put(ctx.getParentID(), resultWriter);
            }
        }
    }

    @Override
    public CommandData deserialize(JsonReader reader) throws IOException {
        // if array, prepare to parse first element
        if (reader.peek() == JsonToken.BEGIN_ARRAY)
            reader.beginArray();

        if (reader.peek() == JsonToken.END_DOCUMENT || !reader.hasNext())
            return null;

        var cmd = CommandData.newBuilder()
                .sender(userID);

        try {
            reader.beginObject();

            while (reader.hasNext()) {
                var propName = reader.nextName();

                if ("id".equals(propName))
                    cmd.id(reader.nextString());
                else if ("control@id".equals(propName))
                    cmd.controlStreamId(reader.nextString());
                else if ("samplingFeature@id".equals(propName))
                    cmd.samplingFeatureId(reader.nextString());
                else if ("issueTime".equals(propName))
                    cmd.issueTime(OffsetDateTime.parse(reader.nextString()).toInstant());
                else if ("params".equals(propName)) {
                    var result = paramsReader.parseNextBlock();
                    cmd.parameters(result);
                } else
                    reader.skipValue();
            }

            reader.endObject();
        } catch (DateTimeParseException e) {
            throw new ResourceParseException(INVALID_JSON_ERROR_MSG + "Invalid ISO8601 date/time at " + reader.getPath());
        } catch (IllegalStateException | ReaderException e) {
            throw new ResourceParseException(INVALID_JSON_ERROR_MSG + e.getMessage());
        }

        var newCmd = cmd.build();

        if (timeStampIndexer != null) {
            var phenomenonTimeIdx = timeStampIndexer.getDataIndex(newCmd.getParameters());
            newCmd.getParameters().setDoubleValue(phenomenonTimeIdx, newCmd.getIssueTime().toEpochMilli() / 1000.0);
        }

        return newCmd;
    }

    @Override
    public void serialize(String commandID, CommandData commandData, boolean showLinks, JsonWriter writer) throws IOException {
        writer.beginObject();

        if (commandID != null) {
            writer.name("id").value(commandID);
        }

        if (commandData.getControlStreamId() != null) {
            writer.name("controlstream@id").value(commandData.getControlStreamId());
        }

        if (commandData.getSamplingFeatureId() != null) {
            writer.name("samplingFeature@id").value(commandData.getSamplingFeatureId());
        }

        if (commandData.getIssueTime() == null)
            writer.name("issueTime").value(Instant.now().toString());
        else
            writer.name("issueTime").value(commandData.getIssueTime().toString());

        writer.name("sender").value(commandData.getSender());

        // create or reuse existing params writer and write param data
        writer.name("parameters");
        var paramWriter = getSweCommonWriter(contextData.csInfo, writer, ctx.getPropertyFilter());

        // write if JSON is supported, otherwise print warning message
        if (paramWriter instanceof JsonDataWriterGson)
            paramWriter.write(commandData.getParameters());
        else
            writer.value("Compressed binary result not shown in JSON");

        writer.endObject();
        writer.flush();
    }

    @Override
    public void startCollection() throws IOException {
        startJsonCollection(writer);
    }

    protected DataStreamWriter getSweCommonWriter(ICommandStreamInfo dsInfo, JsonWriter writer, PropertyFilter propFilter) {
        if (!allowNonBinaryFormat(dsInfo))
            return new BinaryDataWriter();

        // create JSON SWE writer
        var sweWriter = new JsonDataWriterGson(writer);
        sweWriter.setDataComponents(dsInfo.getRecordStructure());

        // filter out time component since it's already included in O&M
        sweWriter.setDataComponentFilter(getTimeStampFilter());
        return sweWriter;
    }

    protected JsonDataParserGson getSweCommonParser(ICommandStreamInfo dsInfo, JsonReader reader) {
        // create JSON SWE parser
        var sweParser = new JsonDataParserGson(reader);
        sweParser.setDataComponents(dsInfo.getRecordStructure());

        // filter out time component since it's already included in O&M
        sweParser.setDataComponentFilter(getTimeStampFilter());
        return sweParser;
    }

    protected IComponentFilter getTimeStampFilter() {
        return new IComponentFilter() {
            @Override
            public boolean accept(DataComponent comp) {
                return comp.getParent() != null &&
                        !SWEConstants.DEF_PHENOMENON_TIME.equals(comp.getDefinition()) &&
                        !SWEConstants.DEF_SAMPLING_TIME.equals(comp.getDefinition());
            }
        };
    }

    protected boolean allowNonBinaryFormat(ICommandStreamInfo dsInfo) {
        if (dsInfo.getRecordEncoding() instanceof BinaryEncoding enc) {
            for (var member : enc.getMemberList()) {
                if (member instanceof BinaryBlock)
                    return false;
            }
        }

        return true;
    }

    @Override
    public void endCollection(Collection<ResourceLink> links) throws IOException {
        endJsonCollection(writer, links);
    }
}
