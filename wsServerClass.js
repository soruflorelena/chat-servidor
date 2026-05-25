import { WebSocketServer } from 'ws'
import { createPool } from 'mariadb'

// ─── CONEXIÓN A BASE DE DATOS ────────────────────────────────────────────────

const pool = createPool({
	host:     process.env.DB_HOST,
	port:     Number(process.env.DB_PORT) || 3306,
	user:     process.env.DB_USER,
	password: process.env.DB_PASSWORD,
	database: process.env.DB_NAME,
	connectionLimit: 5
})

async function query(sql, params = []) {
	let conn
	try {
		conn = await pool.getConnection()
		return await conn.query(sql, params)
	} finally {
		if (conn) conn.release()
	}
}

// Crea las tablas si no existen
async function iniciarBD() {
	await query(`
		CREATE TABLE IF NOT EXISTS mensajes (
			id        INT AUTO_INCREMENT PRIMARY KEY,
			chat      VARCHAR(100) NOT NULL,
			emisor    VARCHAR(50)  NOT NULL,
			texto     TEXT         NOT NULL,
			hora      VARCHAR(10)  NOT NULL,
			leido     BOOLEAN      DEFAULT FALSE,
			creado_en TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
			INDEX idx_chat (chat)
		)
	`)
	await query(`
		CREATE TABLE IF NOT EXISTS grupos (
			id          VARCHAR(50) PRIMARY KEY,
			integrantes TEXT        NOT NULL
		)
	`)
	await query(`
		CREATE TABLE IF NOT EXISTS usuarios (
			nombre VARCHAR(50) PRIMARY KEY
		)
	`)
	console.log("Base de datos lista")
}

// ─── SERVIDOR WEBSOCKET ───────────────────────────────────────────────────────

class wsServer {
	constructor() {
		const port = process.env.PORT || 8080
		this.wss = new WebSocketServer({ port })
		console.log(`Servidor WebSocket iniciado en puerto ${port}`)

		this.wss.on('connection', (ws) => {
			this.MSG(ws, "IDENTIFICATE")

			ws.on('message', async (datos) => {
				datos = this.jsonAJS(datos)
				if (datos) {
					const { mensaje, data } = datos
					if (this[mensaje] && typeof this[mensaje] === "function")
						await this[mensaje](ws, data)
				}
			})

			ws.on('close', () => {
				console.log(`${ws.data} desconectado`)
				this.BROADCAST_CONECTADOS()
			})
		})
	}

	async IDENTIFICACION(ws, data) {
		ws.data = data
		console.log(`${ws.data} conectado...`)

		// Registrar usuario
		await query(
			`INSERT INTO usuarios (nombre) VALUES (?) ON DUPLICATE KEY UPDATE nombre = nombre`,
			[data]
		)

		this.BROADCAST_CONECTADOS()

		// Obtener grupos donde participa para incluir sus mensajes en el historial
		const todosGrupos = await query(`SELECT * FROM grupos`)
		const idsGrupos = todosGrupos
			.filter(g => JSON.parse(g.integrantes).includes(data))
			.map(g => g.id)

		// Historial
		let mensajes = []
		if (idsGrupos.length > 0) {
			const placeholders = idsGrupos.map(() => '?').join(',')
			mensajes = await query(
				`SELECT * FROM mensajes
				 WHERE chat = 'Todos'
				 OR chat LIKE CONCAT('%', ?, '%')
				 OR chat IN (${placeholders})
				 ORDER BY id ASC`,
				[data, ...idsGrupos]
			)
		} else {
			mensajes = await query(
				`SELECT * FROM mensajes
				 WHERE chat = 'Todos'
				 OR chat LIKE CONCAT('%', ?, '%')
				 ORDER BY id ASC`,
				[data]
			)
		}
		this.MSG(ws, "HISTORIAL", mensajes)

		// Enviar todos los usuarios históricos
		const todosUsuarios = await query(`SELECT nombre FROM usuarios`)
		this.MSG(ws, "TODOS_USUARIOS", todosUsuarios.map(u => u.nombre))

		// Enviar grupos donde participa 
		const misGrupos = todosGrupos.filter(g => {
			const integrantes = JSON.parse(g.integrantes)
			return integrantes.includes(data)
		})
		if (misGrupos.length > 0)
			this.MSG(ws, "GRUPOS_INICIALES", misGrupos)
	}

	CONECTADOS() {
		this.BROADCAST_CONECTADOS()
	}

	BROADCAST_CONECTADOS() {
		const identificados = []
		for (const cliente of this.wss.clients)
			if (cliente.data) identificados.push(cliente.data)

		for (const cliente of this.wss.clients) {
			const data = identificados.filter(n => n !== cliente.data)
			this.MSG(cliente, "CONECTADOS", data)
		}
	}

	async CHAT(ws, data) {
		if (!data) return
		const emisor = ws.data
		const { receptor, canal, mensaje } = data
		const esTodos = canal === "Todos"
		const hora = new Date().toLocaleTimeString('es-MX', { hour: '2-digit', minute: '2-digit' })

		let chatId
		if (esTodos) {
			chatId = "Todos"
		} else if (receptor.length === 1) {
			// Chat privado
			chatId = [emisor, receptor[0]].sort().join("|")
		} else {
			// Grupo
			chatId = canal
		}

		// Guardar en BD
		const result = await query(
			`INSERT INTO mensajes (chat, emisor, texto, hora, leido) VALUES (?, ?, ?, ?, ?)`,
			[chatId, emisor, mensaje, hora, false]
		)
		const msgId = Number(result.insertId)

		// Reenviar a destinatarios conectados
		for (const destinatario of receptor) {
			const socket = this.socketId(destinatario)
			if (socket)
				this.MSG(socket, "CHAT", { id: msgId, emisor, mensaje, canal: esTodos ? "Todos" : canal, hora })
		}

		// Echo al emisor en canal Todos
		if (esTodos)
			this.MSG(ws, "CHAT", { id: msgId, emisor, mensaje, canal: "Todos", hora })
	}

	async LEIDO(ws, data) {
		if (!data) return
		const socket = this.socketId(data.emisor)
		if (socket)
			this.MSG(socket, "LEIDO", { lector: ws.data })

		// Marcar como leídos en BD los mensajes de ese chat
		if (data.chatId)
			await query(`UPDATE mensajes SET leido = TRUE WHERE chat = ? AND emisor = ?`, [data.chatId, data.emisor])
	}

	async GRUPO(ws, data) {
		if (!data) return
		const { id, integrantes, accion, integrantesEliminados } = data

		if (accion === "eliminar") {
			await query(`DELETE FROM grupos WHERE id = ?`, [id])
		} else {
			await query(
				`INSERT INTO grupos (id, integrantes) VALUES (?, ?)
				 ON DUPLICATE KEY UPDATE integrantes = VALUES(integrantes)`,
				[id, JSON.stringify(integrantes)]
			)
		}

		// Notificar a integrantes actuales
		for (const nombre of integrantes) {
			const socket = this.socketId(nombre)
			if (socket && socket !== ws)
				this.MSG(socket, "GRUPO", { id, integrantes, accion })
		}

		// Notificar a eliminados
		if (integrantesEliminados?.length > 0) {
			for (const nombre of integrantesEliminados) {
				const socket = this.socketId(nombre)
				if (socket)
					this.MSG(socket, "GRUPO", { id, integrantes: [], accion: "eliminar" })
			}
		}
	}

	socketId(id) {
		for (const cliente of this.wss.clients)
			if (cliente.data === id) return cliente
		return null
	}

	MSG(ws, mensaje, data) {
		const msg = data != null
			? this.JSAJson({ mensaje, data })
			: this.JSAJson({ mensaje })
		if (msg) ws.send(msg)
	}

	jsonAJS(json) {
		try { return JSON.parse(json) }
		catch { return false }
	}

	JSAJson(js) {
		try { return JSON.stringify(js) }
		catch { return false }
	}
}

// Arrancar BD y luego servidor
iniciarBD()
	.then(() => new wsServer())
	.catch(err => { console.error("Error al iniciar BD:", err); process.exit(1) })