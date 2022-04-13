import * as path from "path";
import { v1 } from "uuid";
import axios from "axios";
import { CTraderCommandMap } from "#commands/CTraderCommandMap";
import { CTraderEncoderDecoder } from "#encoder-decoder/CTraderEncoderDecoder";
import { CTraderSocket } from "#sockets/CTraderSocket";
import { GenericObject } from "#utilities/GenericObject";
import { CTraderProtobufReader } from "#protobuf/CTraderProtobufReader";
import { CTraderConnectionParameters } from "#CTraderConnectionParameters";
import { CTraderCommand } from "#commands/CTraderCommand";
import { CTraderLayerEmitter } from "#utilities/emitters/CTraderLayerEmitter";
import { CTraderLayerEventListener } from "#events/CTraderLayerEventListener";
import { CTraderLayerEvent } from "#events/CTraderLayerEvent";

export class CTraderConnection {
    readonly #commandMap: CTraderCommandMap;
    readonly #encoderDecoder: CTraderEncoderDecoder;
    readonly #protobufReader;
    readonly #socket: CTraderSocket;
    readonly #emitter: CTraderLayerEmitter;
    #resolveConnectionPromise?: (...parameters: any[]) => void;
    #rejectConnectionPromise?: (...parameters: any[]) => void;

    public constructor ({ host, port, }: CTraderConnectionParameters) {
        this.#commandMap = new CTraderCommandMap({ send: (data: any): void => this.#send(data), });
        this.#encoderDecoder = new CTraderEncoderDecoder();
        this.#protobufReader = new CTraderProtobufReader([ {
            file: path.resolve(__dirname, "../../../protobuf/OpenApiCommonMessages.proto"),
        }, {
            file: path.resolve(__dirname, "../../../protobuf/OpenApiMessages.proto"),
        }, ]);
        this.#socket = new CTraderSocket({ host, port, });
        this.#emitter = new CTraderLayerEmitter();
        this.#resolveConnectionPromise = undefined;
        this.#rejectConnectionPromise = undefined;

        this.#encoderDecoder.setDecodeHandler((data) => this.#onDecodedData(this.#protobufReader.decode(data)));
        this.#protobufReader.load();
        this.#protobufReader.build();

        this.#socket.onOpen = (): void => this.#onOpen();
        this.#socket.onData = (data: any): void => this.#onData(data);
        this.#socket.onClose = (): void => this.#onClose();
    }

    public getPayloadTypeByName (name: string): number {
        return this.#protobufReader.getPayloadTypeByName(name);
    }

    public getPayloadNameByType (type: number): string {
        return this.#protobufReader.getPayloadNameByType(type);
    }

    async sendCommand (payloadName: string, data?: GenericObject, messageId?: string): Promise<GenericObject> {
        const clientMsgId: string = messageId ?? v1();
        const payloadType: number = this.getPayloadTypeByName(payloadName);
        const message: any = this.#protobufReader.encode(payloadType, data ?? {}, clientMsgId);
        const responsePromise: Promise<GenericObject> = this.#commandMap.create({ clientMsgId, message, });

        if (payloadName.substr(-5).toUpperCase() === "EVENT") {
            const sentCommand: CTraderCommand = this.#commandMap.extractById(clientMsgId) as CTraderCommand;

            sentCommand.resolve({});
        }

        if (payloadName.substr(-3).toUpperCase() === "REQ") {
            const responsePayloadType: number = this.getPayloadTypeByName(`${payloadName.substr(0, payloadName.length - 3)}Res`);

            if (responsePayloadType === -1) {
                const sentCommand: CTraderCommand = this.#commandMap.extractById(clientMsgId) as CTraderCommand;

                sentCommand.resolve({});
            }
        }

        return responsePromise;
    }

    async trySendCommand (payloadName: string, data?: GenericObject, messageId?: string): Promise<GenericObject | undefined> {
        try {
            return await this.sendCommand(payloadName, data, messageId);
        }
        catch {
            return undefined;
        }
    }

    public sendHeartbeat (): void {
        this.sendCommand("ProtoHeartbeatEvent");
    }

    public open (): Promise<unknown> {
        const connectionPromise = new Promise((resolve, reject) => {
            this.#resolveConnectionPromise = resolve;
            this.#rejectConnectionPromise = reject;
        });

        this.#socket.connect();

        return connectionPromise;
    }

    public close (): void {
        this.#socket.disconnect();
    }

    public on (payloadName: string): Promise<CTraderLayerEvent>
    public on (payloadName: string, listener: CTraderLayerEventListener): string
    public on (payloadName: string, listener?: CTraderLayerEventListener): Promise<CTraderLayerEvent> | string {
        const payloadType: string = this.getPayloadTypeByName(payloadName).toString();

        if (!listener) {
            return this.#emitter.on(payloadType);
        }

        return this.#emitter.on(payloadType, listener);
    }

    public removeEventListener (uuid: string): void {
        this.#emitter.removeEventListener(uuid);
    }

    #send (data: GenericObject): void {
        this.#socket.send(this.#encoderDecoder.encode(data));
    }

    #onOpen (): void {
        if (this.#resolveConnectionPromise) {
            this.#resolveConnectionPromise();
        }

        this.#resolveConnectionPromise = undefined;
        this.#rejectConnectionPromise = undefined;
    }

    #onData (data: Buffer): void {
        this.#encoderDecoder.decode(data);
    }

    #onDecodedData (data: GenericObject): void {
        const payloadType = data.payloadType;
        const payload = data.payload;
        const clientMsgId = data.clientMsgId;
        const sentCommand = this.#commandMap.extractById(clientMsgId);
        const normalizedPayload = JSON.parse(payload.encodeJSON());
        const payloadName: string = this.getPayloadNameByType(payloadType);

        if (clientMsgId) {
            normalizedPayload.clientMsgId = clientMsgId;
        }

        if (payloadName.substr(-5).toUpperCase() === "EVENT") {
            this.#onPushEvent(payloadType, normalizedPayload);
        }
        else if (payloadName.substr(-3).toUpperCase() === "RES" && sentCommand) {
            if (typeof payload.errorCode === "string" || typeof payload.errorCode === "number") {
                sentCommand.reject(normalizedPayload);
            }
            else {
                sentCommand.resolve(normalizedPayload);
            }
        }
        else {
            console.log(`Unknown payload type ${payloadType}, ${payloadName}`);
            console.log(normalizedPayload);
        }
    }

    #onClose (): void {
        // Silence is golden
    }

    #onPushEvent (payloadType: number, message: GenericObject): void {
        this.#emitter.notifyListeners(payloadType.toString(), message);
    }

    public static async getAccessTokenProfile (accessToken: string): Promise<GenericObject> {
        const URI = `https://api.spotware.com/connect/profile?access_token=${accessToken}`;

        return JSON.parse((await axios.get(URI)).data);
    }

    public static async getAccessTokenAccounts (accessToken: string): Promise<GenericObject[]> {
        const URI = `https://api.spotware.com/connect/tradingaccounts?access_token=${accessToken}`;
        const parsedResponse: any = JSON.parse((await axios.get(URI)).data);

        if (!Array.isArray(parsedResponse)) {
            return [];
        }

        return parsedResponse;
    }
}
