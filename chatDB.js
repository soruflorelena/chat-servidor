const DB_NAME = 'Chat'
const STORE_GRUPOS = 'grupos'
const STORE_USUARIOS = 'usuarios'
const STORE_MENSAJES = 'mensajes'

export class chatBD {
	constructor() {
		this.db = null
	}

	async init() {
		return new Promise((resolve, reject) => {
			const request = indexedDB.open(DB_NAME, 4)

			request.onupgradeneeded = (e) => {
				const db = e.target.result

				if (!db.objectStoreNames.contains(STORE_GRUPOS))
					db.createObjectStore(STORE_GRUPOS, { keyPath: 'id', autoIncrement: false })

				if (!db.objectStoreNames.contains(STORE_MENSAJES)) {
					const store = db.createObjectStore(STORE_MENSAJES, { keyPath: 'id', autoIncrement: true })
					store.createIndex('chat', 'chat', { unique: false })
				}

				// Store de usuarios que alguna vez se conectaron
				if (!db.objectStoreNames.contains(STORE_USUARIOS))
					db.createObjectStore(STORE_USUARIOS, { keyPath: 'nombre', autoIncrement: false })
			}

			request.onsuccess = (e) => {
				this.db = e.target.result
				resolve()
			}

			request.onerror = (e) => {
				reject(`Error crítico: ${e.target.error.message}`)
			}
		})
	}

	// ─── GRUPOS ───────────────────────────────────────────────────────────────

	async add(id, integrantes) {
		try {
			const tx = this.db.transaction(STORE_GRUPOS, 'readwrite')
			const store = tx.objectStore(STORE_GRUPOS)
			return new Promise((resolve, reject) => {
				const request = store.add({ id, integrantes })
				request.onsuccess = () => resolve(request.result)
				request.onerror = () => reject("No se pudo añadir el grupo")
			})
		} catch (err) {
			console.error("Error en add:", err)
		}
	}

	async getAll() {
		try {
			const tx = this.db.transaction(STORE_GRUPOS, 'readonly')
			const store = tx.objectStore(STORE_GRUPOS)
			return new Promise((resolve) => {
				const request = store.getAll()
				request.onsuccess = () => resolve(request.result)
			})
		} catch (err) {
			console.error("Error en getAll:", err)
			return []
		}
	}

	async update(id, integrantes) {
		try {
			const tx = this.db.transaction(STORE_GRUPOS, 'readwrite')
			const store = tx.objectStore(STORE_GRUPOS)
			return new Promise((resolve, reject) => {
				const request = store.put({ id, integrantes })
				request.onsuccess = () => resolve()
				request.onerror = () => reject("Error al actualizar")
			})
		} catch (err) {
			console.error("Error en update:", err)
		}
	}

	async delete(id) {
		try {
			const tx = this.db.transaction(STORE_GRUPOS, 'readwrite')
			const store = tx.objectStore(STORE_GRUPOS)
			return new Promise((resolve) => {
				const request = store.delete(id)
				request.onsuccess = () => resolve()
			})
		} catch (err) {
			console.error("Error en delete:", err)
		}
	}

	async clearAll() {
		try {
			const tx = this.db.transaction(STORE_GRUPOS, 'readwrite')
			const store = tx.objectStore(STORE_GRUPOS)
			return new Promise((resolve, reject) => {
				const request = store.clear()
				request.onsuccess = () => resolve()
				request.onerror = () => reject("Error al limpiar")
			})
		} catch (err) {
			console.error("Error en clearAll:", err)
		}
	}

	// ─── MENSAJES ─────────────────────────────────────────────────────────────

	async addMensaje(chat, emisor, texto, hora, leido = false) {
		try {
			const tx = this.db.transaction(STORE_MENSAJES, 'readwrite')
			const store = tx.objectStore(STORE_MENSAJES)

			// Verificar si ya existe un mensaje idéntico (mismo chat, emisor, texto y hora)
			// para evitar duplicados causados por StrictMode o reconexiones.
			const index = store.index('chat')
			const existentes = await new Promise(resolve => {
				const req = index.getAll(chat)
				req.onsuccess = () => resolve(req.result)
			})
			const yaDuplicado = existentes.some(
				m => m.emisor === emisor && m.texto === texto && m.hora === hora
			)
			if (yaDuplicado) return

			return new Promise((resolve, reject) => {
				const request = store.add({ chat, emisor, texto, hora, leido })
				request.onsuccess = () => resolve(request.result)
				request.onerror = () => reject("No se pudo guardar el mensaje")
			})
		} catch (err) {
			console.error("Error en addMensaje:", err)
		}
	}

	// Marca todos los mensajes de un chat como leídos
	async marcarLeidos(chat) {
		try {
			const tx = this.db.transaction(STORE_MENSAJES, 'readwrite')
			const store = tx.objectStore(STORE_MENSAJES)
			const index = store.index('chat')

			return new Promise((resolve) => {
				const request = index.getAll(chat)
				request.onsuccess = () => {
					const mensajes = request.result
					for (const msg of mensajes) {
						if (!msg.leido) {
							msg.leido = true
							store.put(msg)
						}
					}
					resolve()
				}
			})
		} catch (err) {
			console.error("Error en marcarLeidos:", err)
		}
	}

	// Si se pasa un chat específico filtra por ese chat,
	// si no se pasa nada trae todos los mensajes
	async getMensajes(chat = null) {
		try {
			const tx = this.db.transaction(STORE_MENSAJES, 'readonly')
			const store = tx.objectStore(STORE_MENSAJES)

			return new Promise((resolve) => {
				let request;
				if (chat) {
					const index = store.index('chat')
					request = index.getAll(chat)
				} else {
					request = store.getAll()
				}
				request.onsuccess = () => resolve(request.result)
			})
		} catch (err) {
			console.error("Error en getMensajes:", err)
			return []
		}
	}
	// ─── USUARIOS ────────────────────────────────────────────────────────────

	// Registra un usuario si no existe todavía
	async addUsuario(nombre) {
		try {
			const tx = this.db.transaction(STORE_USUARIOS, 'readwrite')
			const store = tx.objectStore(STORE_USUARIOS)
			return new Promise((resolve) => {
				// put sobreescribe si ya existe, add fallaría — usamos put para simplicidad
				const request = store.put({ nombre })
				request.onsuccess = () => resolve()
				request.onerror = () => resolve() // silencioso si falla
			})
		} catch (err) {
			console.error("Error en addUsuario:", err)
		}
	}

	// Trae todos los usuarios registrados
	async getUsuarios() {
		try {
			const tx = this.db.transaction(STORE_USUARIOS, 'readonly')
			const store = tx.objectStore(STORE_USUARIOS)
			return new Promise((resolve) => {
				const request = store.getAll()
				request.onsuccess = () => resolve(request.result.map(u => u.nombre))
			})
		} catch (err) {
			console.error("Error en getUsuarios:", err)
			return []
		}
	}
}

window.chatDB = chatBD